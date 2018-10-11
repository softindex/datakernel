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

package io.datakernel.bytebuf;

import io.datakernel.exception.ParseException;
import io.datakernel.util.CharsBuffer;

@SuppressWarnings({"ThrowableInstanceNeverThrown", "WeakerAccess", "unused"})
public final class ByteBufStrings {
	public static final ParseException READ_PAST_LIMIT = new ParseException(ByteBufStrings.class, "Malformed utf-8 input: Read past end");
	public static final ParseException READ_PAST_ARRAY_LENGTH = new ParseException(ByteBufStrings.class, "Malformed utf-8 input");

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
		encodeAscii(buf.array(), buf.writePosition(), string);
		buf.moveWritePosition(string.length());
	}

	public static ByteBuf wrapAscii(String string) {
		ByteBuf buf = ByteBufPool.allocate(string.length());
		byte[] array = buf.array();
		for (int i = 0; i < string.length(); i++) {
			array[i] = (byte) string.charAt(i);
		}
		buf.moveWritePosition(string.length());
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
		return decodeAscii(array, pos, len, CharsBuffer.ensure(len));
	}

	public static String asAscii(ByteBuf buf) {
		String str = decodeAscii(buf.array(), buf.readPosition(), buf.readRemaining(), CharsBuffer.ensure(buf.readRemaining()));
		buf.recycle();
		return str;
	}

	public static String decodeAscii(byte[] array) {
		return decodeAscii(array, 0, array.length, CharsBuffer.ensure(array.length));
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
		toLowerCaseAscii(buf.array(), buf.readPosition(), buf.readRemaining());
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
		toUpperCaseAscii(buf.array(), buf.readPosition(), buf.readRemaining());
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
		return hashCode(buf.array(), buf.readPosition(), buf.readRemaining());
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
		return hashCodeLowerCaseAscii(buf.array(), buf.readPosition(), buf.readRemaining());
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
		return hashCodeUpperCaseAscii(buf.array(), buf.readPosition(), buf.readRemaining());
	}

	// UTF-8
	public static int encodeUtf8(byte[] array, int pos, String string) {
		int p = pos;
		for (int i = 0; i < string.length(); i++) {
			p += encodeUtf8(array, p, string.charAt(i));
		}
		return p - pos;
	}

	public static int encodeUtf8(byte[] array, int pos, char c) {
		int p = pos;
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
		return p - pos;
	}

	public static void putUtf8(ByteBuf buf, String string) {
		int size = encodeUtf8(buf.array(), buf.writePosition(), string);
		buf.moveWritePosition(size);
	}

	public static void putUtf8(ByteBuf buf, char c) {
		int size = encodeUtf8(buf.array(), buf.writePosition(), c);
		buf.moveWritePosition(size);
	}

	public static ByteBuf wrapUtf8(String string) {
		ByteBuf byteBuffer = ByteBufPool.allocate(string.length() * 3);
		int size = encodeUtf8(byteBuffer.array(), 0, string);
		byteBuffer.moveWritePosition(size);
		return byteBuffer;
	}

	public static int decodeUtf8(byte[] array, int pos, int len, char[] buffer, int to) throws ParseException {
		int end = pos + len;
		try {
			while (pos < end) {
				int c = array[pos++] & 0xff;
				switch ((c >> 4) & 0x0F) {
					case 0:
					case 1:
					case 2:
					case 3:
					case 4:
					case 5:
					case 6:
					case 7:
						buffer[to++] = (char) c;
						break;
					case 12:
					case 13:
						buffer[to++] = (char) ((c & 0x1F) << 6 | array[pos++] & 0x3F);
						break;
					case 14:
						buffer[to++] = (char) ((c & 0x0F) << 12 | (array[pos++] & 0x3F) << 6 | (array[pos++] & 0x3F));
						break;
				}
			}
			if (pos > end) throw READ_PAST_LIMIT;
		} catch (ArrayIndexOutOfBoundsException e) {
			throw READ_PAST_ARRAY_LENGTH;
		}
		return to;
	}

	public static String decodeUtf8(byte[] array, int pos, int len, char[] tmpBuffer) throws ParseException {
		int charIndex = 0;
		charIndex = decodeUtf8(array, pos, len, tmpBuffer, charIndex);
		return new String(tmpBuffer, 0, charIndex);
	}

	public static String decodeUtf8(byte[] array, int pos, int len) throws ParseException {
		return decodeUtf8(array, pos, len, CharsBuffer.ensure(len));
	}

	public static String decodeUtf8(byte[] array) throws ParseException {
		return decodeUtf8(array, 0, array.length, CharsBuffer.ensure(array.length));
	}

	public static String asUtf8(ByteBuf buf) throws ParseException {
		String str = decodeUtf8(buf.array(), buf.readPosition(), buf.readRemaining(), CharsBuffer.ensure(buf.readRemaining()));
		buf.recycle();
		return str;
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
		int digits = encodeDecimal(buf.array(), buf.writePosition(), value);
		buf.moveWritePosition(digits);
	}

	public static ByteBuf wrapDecimal(int value) {
		int digits = digits(value);
		ByteBuf buf = ByteBufPool.allocate(digits);
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
			if (b < 0 || b >= 10) {
				throw new ParseException(ByteBufStrings.class, "Not a decimal value" + new String(array, pos, len));
			}
			result = b + result * 10;
			if (result < 0) {
				throw new ParseException(ByteBufStrings.class, "Bigger than max int value: " + new String(array, pos, len));
			}
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
