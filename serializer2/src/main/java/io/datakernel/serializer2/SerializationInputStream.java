/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.serializer2;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static java.lang.Math.min;
import static java.lang.System.arraycopy;

public final class SerializationInputStream extends InputStream {

	private final InputStream inputStream;
	private final byte[] buf;
	private int pos;
	private int limit;

	private char[] charArray = new char[128];

	public SerializationInputStream(InputStream inputStream, int bufferSize) {
		this.inputStream = inputStream;
		this.buf = new byte[bufferSize];
	}

	private void doEnsureSize(int size) throws IOException {
		int len = limit - pos;
		arraycopy(buf, pos, buf, 0, len);
		pos = 0;
		limit = len;
		while (size() < size) {
			int read = inputStream.read(buf, limit, buf.length - limit);
			if (read == -1)
				throw new EOFException();
			limit += read;
		}
	}

	private void ensureSize(int size) throws IOException {
		if (size() < size) {
			doEnsureSize(size);
		}
	}

	@Override
	public int read() throws IOException {
		return readByte() & 0xFF;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int size = size();
		if (size > 0) {
			int read = min(len, size);
			arraycopy(buf, pos, b, off, read);
			pos += read;
			if (pos == limit) {
				pos = 0;
				limit = 0;
			}
			return read;
		} else {
			if (len < buf.length) {
				pos = limit = 0;
				while (size() < len) {
					int read = inputStream.read(buf, limit, buf.length - limit);
					if (read == -1)
						throw new EOFException();
					limit += read;
				}
				arraycopy(buf, pos, b, off, len);
				pos += len;
				if (pos == limit) {
					pos = 0;
					limit = 0;
				}
				return len;
			} else {
				return inputStream.read(b, off, len);
			}
		}
	}

	@Override
	public int available() throws IOException {
		int remaining = size();
		if (remaining > 0)
			return remaining;
		return inputStream.available();
	}

	@Override
	public void close() throws IOException {
		inputStream.close();
	}

	private int size() {
		return limit - pos;
	}

	private char[] ensureCharArray(int length) {
		if (charArray.length < length) {
			charArray = new char[length + (length >>> 2)];
		}
		return charArray;
	}

	public byte readByte() throws IOException {
		ensureSize(1);
		return buf[pos++];
	}

	public void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}

	public void readFully(byte[] b, int off, int len) throws IOException {
		ensureSize(len);
		arraycopy(buf, pos, b, off, len);
		pos += len;
	}

	public boolean readBoolean() throws IOException {
		return readByte() != 0;
	}

	public char readChar() throws IOException {
		ensureSize(2);
		int ch1 = buf[pos];
		int ch2 = buf[pos + 1];
		pos += 2;
		int code = (ch1 << 8) + (ch2 & 0xFF);
		return (char) code;
	}

	public double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
	}

	public float readFloat() throws IOException {
		return Float.intBitsToFloat(readInt());
	}

	public int readInt() throws IOException {
		ensureSize(4);
		int result = ((buf[pos] & 0xFF) << 24)
				| ((buf[pos + 1] & 0xFF) << 16)
				| ((buf[pos + 2] & 0xFF) << 8)
				| (buf[pos + 3] & 0xFF);
		pos += 4;
		return result;
	}

	public int readVarInt() throws IOException {
		int result;
		ensureSize(1);
		byte b = buf[pos];
		if (b >= 0) {
			result = b;
			pos += 1;
		} else {
			result = b & 0x7f;
			ensureSize(2);
			if ((b = buf[pos + 1]) >= 0) {
				result |= b << 7;
				pos += 2;
			} else {
				result |= (b & 0x7f) << 7;
				ensureSize(3);
				if ((b = buf[pos + 2]) >= 0) {
					result |= b << 14;
					pos += 3;
				} else {
					result |= (b & 0x7f) << 14;
					ensureSize(4);
					if ((b = buf[pos + 3]) >= 0) {
						result |= b << 21;
						pos += 4;
					} else {
						result |= (b & 0x7f) << 21;
						ensureSize(5);
						if ((b = buf[pos + 4]) >= 0) {
							result |= b << 28;
							pos += 5;
						} else
							throw new IllegalArgumentException();
					}
				}
			}
		}
		return result;
	}

	public long readLong() throws IOException {
		ensureSize(8);
		long result = ((long) buf[pos] << 56)
				| ((long) (buf[pos + 1] & 0xFF) << 48)
				| ((long) (buf[pos + 2] & 0xFF) << 40)
				| ((long) (buf[pos + 3] & 0xFF) << 32)
				| ((long) (buf[pos + 4] & 0xFF) << 24)
				| ((buf[pos + 5] & 0xFF) << 16)
				| ((buf[pos + 6] & 0xFF) << 8)
				| ((buf[pos + 7] & 0xFF));

		pos += 8;
		return result;
	}

	public short readShort() throws IOException {
		ensureSize(2);
		short result = (short) (((buf[pos] & 0xFF) << 8)
				| ((buf[pos + 1] & 0xFF)));
		pos += 2;
		return result;
	}

	public String readUTF8() throws IOException {
		int length = readVarInt();
		return doReadUTF8(length);
	}

	public String readNullableUTF8() throws IOException {
		int length = readVarInt();
		if (length == 0)
			return null;
		return doReadUTF8(length - 1);
	}

	private String doReadUTF8(int length) throws IOException {
		if (length == 0)
			return "";
		if (length > buf.length)
			throw new IllegalArgumentException();
		char[] chars = ensureCharArray(length);
		for (int i = 0; i < length; i++) {
			int c = readByte() & 0xff;
			if (c < 0x80) {
				chars[i] = (char) c;
			} else if (c < 0xE0) {
				chars[i] = (char) ((c & 0x1F) << 6 | readByte() & 0x3F);
			} else {
				chars[i] = (char) ((c & 0x0F) << 12 | (readByte() & 0x3F) << 6 | (readByte() & 0x3F));
			}
		}
		return new String(chars, 0, length);
	}

	public String readUTF16() throws IOException {
		int length = readVarInt();
		return doReadUTF16(length);
	}

	public String readNullableUTF16() throws IOException {
		int length = readVarInt();
		if (length == 0) {
			return null;
		}
		return doReadUTF16(length - 1);
	}

	private String doReadUTF16(int length) throws IOException {
		if (length * 2 > buf.length)
			throw new IllegalArgumentException();
		char[] chars = ensureCharArray(length);
		for (int i = 0; i < length; i++) {
			chars[i] = (char) ((readByte() << 8) + (readByte()));
		}
		return new String(chars, 0, length);
	}

	public long readVarLong() throws IOException {
		long result = 0;
		for (int offset = 0; offset < 64; offset += 7) {
			byte b = readByte();
			result |= (long) (b & 0x7F) << offset;
			if ((b & 0x80) == 0)
				return result;
		}
		throw new IllegalArgumentException();
	}

}
