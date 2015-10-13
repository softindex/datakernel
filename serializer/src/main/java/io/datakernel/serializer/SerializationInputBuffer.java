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

import java.io.UnsupportedEncodingException;

public final class SerializationInputBuffer {

	private byte[] buf;
	private int pos;

	private char[] charArray = new char[128];

	public SerializationInputBuffer() {
	}

	public SerializationInputBuffer(byte[] array, int position) {
		set(array, position);
	}

	public void set(byte[] array, int position) {
		this.buf = array;
		this.pos = position;
//		if (pos > buf.length)
//			throw new IllegalArgumentException();
	}

	public byte[] array() {
		return buf;
	}

	public int position() {
		return pos;
	}

	public void position(int position) {
		pos = position;
	}

	private int remaining() {
		return buf.length - pos;
	}

	public void skip(int length) {
		pos += length;
	}

	private char[] ensureCharArray(int length) {
		if (charArray.length < length) {
			charArray = new char[length + (length >>> 2)];
		}
		return charArray;
	}

	public byte readByte() {
		return buf[pos++];
	}

	public int read(byte[] b) {
		return read(b, 0, b.length);
	}

	public int read(byte[] b, int off, int len) {
		System.arraycopy(buf, pos, b, off, len);
		pos += len;
		return len;
	}

	public boolean readBoolean() {
		return readByte() != 0;
	}

	public char readChar() {
		int ch1 = readByte();
		int ch2 = readByte();
		int code = (ch1 << 8) + (ch2 & 0xFF);
		return (char) code;
	}

	public double readDouble() {
		return Double.longBitsToDouble(readLong());
	}

	public float readFloat() {
		return Float.intBitsToFloat(readInt());
	}

	public int readInt() {
		int result = ((buf[pos] & 0xFF) << 24)
				| ((buf[pos + 1] & 0xFF) << 16)
				| ((buf[pos + 2] & 0xFF) << 8)
				| (buf[pos + 3] & 0xFF);
		pos += 4;
		return result;
	}

	public int readVarInt() {
		int result;
		byte b = buf[pos];
		if (b >= 0) {
			result = b;
			pos += 1;
		} else {
			result = b & 0x7f;
			if ((b = buf[pos + 1]) >= 0) {
				result |= b << 7;
				pos += 2;
			} else {
				result |= (b & 0x7f) << 7;
				if ((b = buf[pos + 2]) >= 0) {
					result |= b << 14;
					pos += 3;
				} else {
					result |= (b & 0x7f) << 14;
					if ((b = buf[pos + 3]) >= 0) {
						result |= b << 21;
						pos += 4;
					} else {
						result |= (b & 0x7f) << 21;
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

	public long readLong() {
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

	public short readShort() {
		short result = (short) (((buf[pos] & 0xFF) << 8)
				| ((buf[pos + 1] & 0xFF)));
		pos += 2;
		return result;
	}

	public String readIso88591() {
		int length = readVarInt();
		return doReadIso88591(length);
	}

	public String readNullableIso88591() {
		int length = readVarInt();
		if (length == 0)
			return null;
		return doReadIso88591(length - 1);
	}

	public String doReadIso88591(int length) {
		if (length == 0)
			return "";
		if (length > remaining())
			throw new IllegalArgumentException();

		char[] chars = ensureCharArray(length);
		for (int i = 0; i < length; i++) {
			int c = readByte() & 0xff;
			chars[i] = (char) c;
		}
		return new String(chars, 0, length);
	}

	public String readUTF8() {
		int length = readVarInt();
		return doReadUTF8(length);
	}

	public String readNullableUTF8() {
		int length = readVarInt();
		if (length == 0)
			return null;
		return doReadUTF8(length - 1);
	}

	private String doReadUTF8(int length) {
		if (length == 0)
			return "";
		if (length > remaining())
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

	public String readUTF16() {
		int length = readVarInt();
		return doReadUTF16(length);
	}

	public String readNullableUTF16() {
		int length = readVarInt();
		if (length == 0) {
			return null;
		}
		return doReadUTF16(length - 1);
	}

	private String doReadUTF16(int length) {
		if (length == 0)
			return "";
		if (length * 2 > remaining())
			throw new IllegalArgumentException();

		char[] chars = ensureCharArray(length);
		for (int i = 0; i < length; i++) {
			chars[i] = (char) ((readByte() << 8) + (readByte()));
		}
		return new String(chars, 0, length);
	}

	public long readVarLong() {
		long result = 0;
		for (int offset = 0; offset < 64; offset += 7) {
			byte b = readByte();
			result |= (long) (b & 0x7F) << offset;
			if ((b & 0x80) == 0)
				return result;
		}
		throw new IllegalArgumentException();
	}

	public String readJavaUTF8() {
		int length = readVarInt();
		return doReadJavaUTF8(length);
	}

	public String readNullableJavaUTF8() {
		int length = readVarInt();
		if (length == 0) {
			return null;
		}
		return doReadJavaUTF8(length - 1);
	}

	public String doReadJavaUTF8(int length) {
		if (length == 0)
			return "";
		if (length > remaining())
			throw new IllegalArgumentException();
		pos += length;

		try {
			return new String(buf, pos - length, length, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException();
		}
	}
}
