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

package io.datakernel.serializer;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

public final class DataInputStream implements Closeable {
	public static final int DEFAULT_BUFFER_SIZE = 65536;

	private InputStream inputStream;
	private final SerializationInputBuffer inputBuffer = new SerializationInputBuffer();
	private byte[] buf;
	private int pos;
	private int limit;

	private char[] charArray = new char[128];

	public DataInputStream(InputStream inputStream) {
		this(inputStream, DEFAULT_BUFFER_SIZE);
	}

	public DataInputStream(InputStream inputStream, int bufferSize) {
		this.inputStream = inputStream;
		this.buf = new byte[bufferSize];
		this.inputBuffer.set(buf, 0);
	}

	public void changeInputStream(InputStream inputStream) throws IOException {
		if (this.inputStream != null) {
			this.inputStream.close();
		}
		this.inputStream = inputStream;
	}

	private void ensureReadSize(int size) {
		if (buf.length - pos >= size) {
			return;
		}
		byte[] newBuffer = buf.length >= size ? buf : new byte[Math.max(buf.length, size + size / 2)];
		System.arraycopy(buf, pos, newBuffer, 0, limit - pos);
		buf = newBuffer;
		limit -= pos;
		pos = 0;
		inputBuffer.set(buf, 0);
	}

	public byte[] getBuffer() {
		return buf;
	}

	public int getPos() {
		return pos;
	}

	public int getLimit() {
		return limit;
	}

	private void doEnsureRead(int size) throws IOException {
		while (limit - pos < size) {
			ensureReadSize(size);
			int bytesRead = inputStream.read(buf, limit, buf.length - limit);
			if (bytesRead == -1)
				throw new IOException("Could not read message");
			limit += bytesRead;
		}
	}

	public void ensureRead(int size) throws IOException {
		if (limit - pos < size) {
			doEnsureRead(size);
		}
	}

	private int readSize() throws IOException {
		int result;
		byte b = readByte();
		if (b >= 0) {
			result = b;
		} else {
			result = b & 0x7f;
			if ((b = readByte()) >= 0) {
				result |= b << 7;
			} else {
				result |= (b & 0x7f) << 7;
				if ((b = readByte()) >= 0) {
					result |= b << 14;
				} else {
					throw new IOException();
				}
			}
		}
		return result;
	}

	@Override
	public void close() throws IOException {
		inputStream.close();
		this.inputBuffer.set(null, 0);
	}

	private boolean doCheckEndOfStream() throws IOException {
		int bytesRead = inputStream.read(buf, 0, buf.length);
		if (bytesRead == -1)
			return true;
		assert buf.length != 0 && bytesRead != 0;
		pos = 0;
		limit = bytesRead;
		return false;
	}

	public boolean isEndOfStream() throws IOException {
		if (pos != limit)
			return false;
		return doCheckEndOfStream();
	}

	public <T> T deserialize(BufferSerializer<T> serializer) throws IOException, DeserializeException {
		int messageSize = readSize();

		ensureRead(messageSize);

		inputBuffer.position(pos);
		T item;
		try {
			item = serializer.deserialize(inputBuffer);
		} catch (Exception e) {
			throw new DeserializeException(e);
		} finally {
			pos += messageSize;
		}
		if (inputBuffer.position() != pos) {
			throw new DeserializeException("Deserialized size != parsed data size");
		}
		return item;
	}

	private char[] ensureCharArray(int length) {
		if (charArray.length < length) {
			charArray = new char[length + (length >>> 2)];
		}
		return charArray;
	}

	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	public int read(byte[] b, int off, int len) throws IOException {
		ensureRead(len);
		System.arraycopy(buf, pos, b, off, len);
		pos += len;
		return len;
	}

	public byte readByte() throws IOException {
		if (pos == limit) {
			ensureRead(1);
		}
		return buf[pos++];
	}

	public boolean readBoolean() throws IOException {
		return readByte() != 0;
	}

	public short readShort() throws IOException {
		ensureRead(2);
		short result = (short) (((buf[pos] & 0xFF) << 8)
				| ((buf[pos + 1] & 0xFF)));
		pos += 2;
		return result;
	}

	public int readInt() throws IOException {
		ensureRead(4);
		int result = ((buf[pos] & 0xFF) << 24)
				| ((buf[pos + 1] & 0xFF) << 16)
				| ((buf[pos + 2] & 0xFF) << 8)
				| (buf[pos + 3] & 0xFF);
		pos += 4;
		return result;
	}

	public long readLong() throws IOException {
		ensureRead(8);
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

	public int readVarInt() throws IOException {
		int result;
		byte b = readByte();
		if (b >= 0) {
			result = b;
		} else {
			result = b & 0x7f;
			if ((b = readByte()) >= 0) {
				result |= b << 7;
			} else {
				result |= (b & 0x7f) << 7;
				if ((b = readByte()) >= 0) {
					result |= b << 14;
				} else {
					result |= (b & 0x7f) << 14;
					if ((b = readByte()) >= 0) {
						result |= b << 21;
					} else {
						result |= (b & 0x7f) << 21;
						if ((b = readByte()) >= 0) {
							result |= b << 28;
						} else
							throw new IllegalArgumentException();
					}
				}
			}
		}
		return result;
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

	public float readFloat() throws IOException {
		return Float.intBitsToFloat(readInt());
	}

	public double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
	}

	public char readChar() throws IOException {
		return (char) readShort();
	}

	public String readUTF8() throws IOException {
		int length = readVarInt();
		if (length == 0)
			return "";
		ensureRead(length);
		pos += length;

		try {
			return new String(buf, pos - length, length, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException();
		}
	}

	public String readIso88591() throws IOException {
		int length = readVarInt();
		if (length == 0)
			return "";
		ensureRead(length);

		char[] chars = ensureCharArray(length);
		for (int i = 0; i < length; i++) {
			int c = readByte() & 0xff;
			chars[i] = (char) c;
		}
		return new String(chars, 0, length);
	}

	public String readUTF16() throws IOException {
		int length = readVarInt();
		if (length == 0)
			return "";
		ensureRead(length * 2);

		char[] chars = ensureCharArray(length);
		for (int i = 0; i < length; i++) {
			byte b1 = buf[pos++];
			byte b2 = buf[pos++];
			chars[i] = (char) (((b1 & 0xFF) << 8) + (b2 & 0xFF));
		}
		return new String(chars, 0, length);
	}

}
