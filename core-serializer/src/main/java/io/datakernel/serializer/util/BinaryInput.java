/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.serializer.util;

import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Provides methods for reading primitives
 * and Strings from byte arrays
 */
public final class BinaryInput {
	private final byte[] array;
	private int pos;

	public BinaryInput(byte[] array) {
		this.array = array;
	}

	public BinaryInput(byte[] array, int pos) {
		this.array = array;
		this.pos = pos;
	}

	public byte[] array() {
		return array;
	}

	public int pos() {
		return pos;
	}

	public void pos(int pos) {
		this.pos = pos;
	}

	public int read(byte[] b) {
		return read(b, 0, b.length);
	}

	public int read(byte[] b, int off, int len) {
		System.arraycopy(this.array, pos, b, off, len);
		pos += len;
		return len;
	}

	public byte readByte() {
		return array[pos++];
	}

	public boolean readBoolean() {
		return readByte() != 0;
	}

	public short readShort() {
		short result = (short) (((array[pos] & 0xFF) << 8)
				| (array[pos + 1] & 0xFF));
		pos += 2;
		return result;
	}

	public int readInt() {
		int result = ((array[pos] & 0xFF) << 24)
				| ((array[pos + 1] & 0xFF) << 16)
				| ((array[pos + 2] & 0xFF) << 8)
				| (array[pos + 3] & 0xFF);
		pos += 4;
		return result;
	}

	public long readLong() {
		long result = ((long) array[pos] << 56)
				| ((long) (array[pos + 1] & 0xFF) << 48)
				| ((long) (array[pos + 2] & 0xFF) << 40)
				| ((long) (array[pos + 3] & 0xFF) << 32)
				| ((long) (array[pos + 4] & 0xFF) << 24)
				| ((array[pos + 5] & 0xFF) << 16)
				| ((array[pos + 6] & 0xFF) << 8)
				| (array[pos + 7] & 0xFF);
		pos += 8;
		return result;
	}

	public int readVarInt() {
		int result;
		byte b = array[pos];
		if (b >= 0) {
			result = b;
			pos += 1;
		} else {
			result = b & 0x7f;
			if ((b = array[pos + 1]) >= 0) {
				result |= b << 7;
				pos += 2;
			} else {
				result |= (b & 0x7f) << 7;
				if ((b = array[pos + 2]) >= 0) {
					result |= b << 14;
					pos += 3;
				} else {
					result |= (b & 0x7f) << 14;
					if ((b = array[pos + 3]) >= 0) {
						result |= b << 21;
						pos += 4;
					} else {
						result |= (b & 0x7f) << 21;
						if ((b = array[pos + 4]) >= 0) {
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

	public float readFloat() {
		return Float.intBitsToFloat(readInt());
	}

	public double readDouble() {
		return Double.longBitsToDouble(readLong());
	}

	public char readChar() {
		char c = (char) (((array[pos] & 0xFF) << 8) | (array[pos + 1] & 0xFF));
		pos += 2;
		return c;
	}

	public String readIso88591() {
		int length = readVarInt();
		return doReadIso88591(length);
	}

	@Nullable
	public String readIso88591Nullable() {
		int length = readVarInt();
		if (length == 0) {
			return null;
		}
		return doReadIso88591(length - 1);
	}

	private String doReadIso88591(int length) {
		if (length == 0) {
			return "";
		}
		char[] chars = new char[length];
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

	private String doReadUTF8(int length) {
		if (length == 0)
			return "";
		pos += length;
		return new String(array, pos - length, length, UTF_8);
	}

	public String readUTF8mb3() {
		int length = readVarInt();
		return doReadUTF8mb3(length);
	}

	@Nullable
	public String readUTF8mb3Nullable() {
		int length = readVarInt();
		if (length == 0) {
			return null;
		}
		return doReadUTF8mb3(length - 1);
	}

	private String doReadUTF8mb3(int length) {
		if (length == 0) {
			return "";
		}
		char[] chars = new char[length];
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

	@Nullable
	public String readUTF16Nullable() {
		int length = readVarInt();
		if (length == 0) {
			return null;
		}
		return doReadUTF16(length - 1);
	}

	private String doReadUTF16(int length) {
		if (length == 0) {
			return "";
		}
		char[] chars = new char[length];
		for (int i = 0; i < length; i++) {
			chars[i] = (char) ((readByte() << 8) + readByte());
		}
		return new String(chars, 0, length);
	}

	@Nullable
	public String readUTF8Nullable() {
		int length = readVarInt();
		if (length == 0) {
			return null;
		}
		return doReadUTF8(length - 1);
	}

}
