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

package io.datakernel.util;

import io.datakernel.async.ParseException;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;

public final class ByteBufStrings {
	public static final byte CR = (byte) '\r';
	public static final byte LF = (byte) '\n';
	public static final byte SP = (byte) ' ';
	public static final byte HT = (byte) '\t';

	private ByteBufStrings() {
	}

	// ASCII

	public static void encodeAscii(byte[] array, int pos, String string) {
		for (int i = 0; i < string.length(); i++) {
			array[pos++] = (byte) string.charAt(i);
		}
	}

	public static byte[] encodeAscii(String string) {
		byte[] array = new byte[string.length()];
		for (int i = 0; i < string.length(); i++) {
			array[i] = (byte) string.charAt(i);
		}
		return array;
	}

	public static void putAscii(ByteBuf buf, String string) {
		encodeAscii(buf.array(), buf.position(), string);
		buf.advance(string.length());
	}

	public static ByteBuf wrapAscii(String string) {
		ByteBuf buf = ByteBufPool.allocate(string.length());
		byte[] array = buf.array();
		for (int i = 0; i < string.length(); i++) {
			array[i] = (byte) string.charAt(i);
		}
		return buf;
	}

	public static String decodeAscii(byte[] array, int pos, int len, char[] tmpBuffer) {
		int charIndex = 0, end = pos + len;
		while (pos < end) {
			int c = array[pos++] & 0xff;
			tmpBuffer[charIndex++] = (char) c;
		}
		return new String(tmpBuffer, 0, charIndex);
	}

	public static String decodeAscii(byte[] array, int pos, int len) {
		return decodeAscii(array, pos, len, new char[len]);
	}

	public static String decodeAscii(ByteBuf buf, char[] tmpBuffer) {
		return decodeAscii(buf.array(), buf.position(), buf.remaining(), tmpBuffer);
	}

	public static String decodeAscii(ByteBuf buf) {
		return decodeAscii(buf.array(), buf.position(), buf.remaining(), new char[buf.remaining()]);
	}

	public static String decodeAscii(byte[] array) {
		return decodeAscii(array, 0, array.length, new char[array.length]);
	}

	public static void toLowerCaseAscii(byte[] bytes, int pos, int len) {
		for (int i = pos; i < pos + len; i++) {
			byte b = bytes[i];
			if (b >= 'A' && b <= 'Z') {
				bytes[i] = (byte) (b + ((byte) 'a' - (byte) 'A'));
			}
		}
	}

	public static void toLowerCaseAscii(byte[] bytes) {
		toLowerCaseAscii(bytes, 0, bytes.length);
	}

	public static void toLowerCaseAscii(ByteBuf buf) {
		toLowerCaseAscii(buf.array(), buf.position(), buf.remaining());
	}

	public static void toUpperCaseAscii(byte[] bytes, int pos, int len) {
		for (int i = pos; i < pos + len; i++) {
			byte b = bytes[i];
			if (b >= 'a' && b <= 'z') {
				bytes[i] = (byte) (b + ((byte) 'A' - (byte) 'a'));
			}
		}
	}

	public static void toUpperCaseAscii(byte[] bytes) {
		toUpperCaseAscii(bytes, 0, bytes.length);
	}

	public static void toUpperCaseAscii(ByteBuf buf) {
		toUpperCaseAscii(buf.array(), buf.position(), buf.remaining());
	}

	public static boolean equalsLowerCaseAscii(byte[] lowerCasePattern, byte[] array, int offset, int size) {
		if (lowerCasePattern.length != size)
			return false;
		for (int i = 0; i < lowerCasePattern.length; i++) {
			byte p = lowerCasePattern[i];
			assert p < 'A' || p > 'Z';
			byte a = array[offset + i];
			if (a >= 'A' && a <= 'Z')
				a += 'a' - 'A';
			if (a != p)
				return false;
		}
		return true;
	}

	public static boolean equalsUpperCaseAscii(byte[] upperCasePattern, byte[] array, int offset, int size) {
		if (upperCasePattern.length != size)
			return false;
		for (int i = 0; i < upperCasePattern.length; i++) {
			byte p = upperCasePattern[i];
			assert p < 'a' || p > 'z';
			byte a = array[offset + i];
			if (a >= 'a' && a <= 'z')
				a += 'A' - 'z';
			if (a != p)
				return false;
		}
		return true;
	}

	public static int hashCode(byte[] array, int offset, int size) {
		int result = 1;
		for (int i = offset; i < offset + size; i++) {
			byte a = array[i];
			result = 31 * result + a;
		}
		return result;
	}

	public static int hashCode(byte[] array) {
		return hashCode(array, 0, array.length);
	}

	public static int hashCode(ByteBuf buf) {
		return hashCode(buf.array(), buf.position(), buf.remaining());
	}

	public static int hashCodeLowerCaseAscii(byte[] array, int offset, int size) {
		int result = 1;
		for (int i = offset; i < offset + size; i++) {
			byte a = array[i];
			if (a >= 'A' && a <= 'Z')
				a += 'a' - 'A';
			result = 31 * result + a;
		}
		return result;
	}

	public static int hashCodeLowerCaseAscii(byte[] array) {
		return hashCodeLowerCaseAscii(array, 0, array.length);
	}

	public static int hashCodeLowerCaseAscii(ByteBuf buf) {
		return hashCodeLowerCaseAscii(buf.array(), buf.position(), buf.remaining());
	}

	public static int hashCodeUpperCaseAscii(byte[] array, int offset, int size) {
		int result = 1;
		for (int i = offset; i < offset + size; i++) {
			byte a = array[i];
			if (a >= 'a' && a <= 'z')
				a += 'A' - 'a';
			result = 31 * result + a;
		}
		return result;
	}

	public static int hashCodeUpperCaseAscii(byte[] array) {
		return hashCodeUpperCaseAscii(array, 0, array.length);
	}

	public static int hashCodeUpperCaseAscii(ByteBuf buf) {
		return hashCodeUpperCaseAscii(buf.array(), buf.position(), buf.remaining());
	}

	// UTF-8

	public static int encodeUTF8(byte[] array, int pos, String string) {
		int c, p = pos;
		for (int i = 0; i < string.length(); i++) {
			c = string.charAt(i);
			if (c <= 0x007F) {
				array[p++] = (byte) c;
			} else if (c > 0x07FF) {
				array[p++] = ((byte) (0xE0 | c >> 12 & 0x0F));
				array[p++] = ((byte) (0x80 | c >> 6 & 0x3F));
				array[p++] = ((byte) (0x80 | c & 0x3F));
			} else {
				array[p++] = ((byte) (0xC0 | c >> 6 & 0x1F));
				array[p++] = ((byte) (0x80 | c & 0x3F));
			}
		}
		return p - pos;
	}

	public static void putUTF8(ByteBuf buf, String string) {
		int size = encodeUTF8(buf.array(), buf.position(), string);
		buf.advance(size);
	}

	public static ByteBuf wrapUTF8(String string) {
		ByteBuf byteBuffer = ByteBufPool.allocate(string.length() * 3);
		int len = encodeUTF8(byteBuffer.array(), 0, string);
		byteBuffer.limit(len);
		return byteBuffer;
	}

	public static String decodeUTF8(byte[] array, int pos, int len, char[] tmpBuffer) throws ParseException {
		int c, charIndex = 0, end = pos + len;
		try {
			while (pos < end) {
				c = array[pos++] & 0xff;
				switch ((c >> 4) & 0x0F) {
					case 0:
					case 1:
					case 2:
					case 3:
					case 4:
					case 5:
					case 6:
					case 7:
						tmpBuffer[charIndex++] = (char) c;
						break;
					case 12:
					case 13:
						tmpBuffer[charIndex++] = (char) ((c & 0x1F) << 6 | array[pos++] & 0x3F);
						break;
					case 14:
						tmpBuffer[charIndex++] = (char) ((c & 0x0F) << 12 | (array[pos++] & 0x3F) << 6 | (array[pos++] & 0x3F));
						break;
				}
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new ParseException("Malformed utf-8 input", e);
		}
		return new String(tmpBuffer, 0, charIndex);
	}

	public static String decodeUTF8(byte[] array, int pos, int len) throws ParseException {
		return decodeUTF8(array, pos, len, new char[len]);
	}

	public static String decodeUTF8(ByteBuf buf, char[] tmpBuffer) throws ParseException {
		return decodeUTF8(buf.array(), buf.position(), buf.remaining(), tmpBuffer);
	}

	public static String decodeUTF8(ByteBuf buf) throws ParseException {
		return decodeUTF8(buf.array(), buf.position(), buf.remaining(), new char[buf.remaining()]);
	}

	public static String decodeUTF8(byte[] array) throws ParseException {
		return decodeUTF8(array, 0, array.length, new char[array.length]);
	}

	// Decimal (unsigned)

	public static int encodeDecimal(byte[] array, int pos, int value) {
		int digits = digits(value);
		for (int i = pos + digits - 1; i >= pos; i--) {
			int digit = value % 10;
			value = value / 10;
			array[i] = (byte) ('0' + digit);
		}
		return digits;
	}

	public static void putDecimal(ByteBuf buf, int value) {
		int digits = encodeDecimal(buf.array(), buf.position(), value);
		buf.advance(digits);
	}

	public static ByteBuf wrapDecimal(int value) {
		int digits = digits(value);
		ByteBuf buf = ByteBuf.allocate(digits);
		byte[] array = buf.array();
		for (int i = digits - 1; i >= 0; i--) {
			int digit = value % 10;
			value = value / 10;
			array[i] = (byte) ('0' + digit);
		}
		return buf;
	}

	public static int decodeDecimal(byte[] array, int pos, int len) throws ParseException {
		int result = 0;
		int offsetLeft = trimOffsetLeft(array, pos, len);
		pos = pos + offsetLeft;
		len = len - offsetLeft;
		len = len - trimOffsetRight(array, pos, len);
		for (int i = pos; i < pos + len; i++) {
			byte b = (byte) (array[i] - '0');
			if (b < 0 || b >= 10)
				throw new ParseException("Not a decimal value" + new String(array, pos, len));
			result = b + result * 10;
		}
		return result;
	}

	private static int trimOffsetLeft(byte[] array, int pos, int len) {
		for (int i = 0; i < len; i++) {
			if (array[pos + i] != SP && array[pos + i] != HT)
				return i;
		}
		return 0;
	}

	private static int trimOffsetRight(byte[] array, int pos, int len) {
		for (int i = len - 1; i >= 0; i--) {
			if (array[pos + i] != SP && array[pos + i] != HT)
				return len - i - 1;
		}
		return 0;
	}

	private static int digits(int x) {
		int limit = 10;
		for (int i = 1; i <= 9; i++) {
			if (x < limit)
				return i;
			limit *= 10;
		}
		return 10;
	}
}