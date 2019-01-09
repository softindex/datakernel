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

package io.datakernel.http;

import io.datakernel.exception.ParseException;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.Map;

import static io.datakernel.bytebuf.ByteBufStrings.*;

/**
 * Util for working with {@link HttpRequest}
 */
public final class HttpUtils {
	public static final ParseException INVALID_Q_VALUE = new ParseException("Value of 'q' should start either from 0 or 1");
	public static final ParseException ENCODE_NEGATIVE_VALUE = new ParseException("Trying to encode value that is less than 0");
	public static final ParseException DECODE_NEGATIVE_VALUE = new ParseException("Trying to decode negative value");
	private static final String ENCODING = "UTF-8";

	public static InetAddress inetAddress(String host) {
		try {
			return InetAddress.getByName(host);
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e);
		}
	}

	// https://url.spec.whatwg.org/
	public static boolean isInetAddress(String host) {
		int colons = 0;
		int dots = 0;
		byte[] bytes = encodeAscii(host);

		// expect ipv6 address
		if (bytes[0] == '[') {
			return bytes[bytes.length - 1] == ']' && checkIpv6(bytes, 1, bytes.length - 1);
		}

		// assume ipv4 could be as oct, bin, dec; ipv6 - hex
		for (byte b : bytes) {
			if (b == '.') {
				dots++;
			} else if (b == ':') {
				if (dots != 0) {
					return false;
				}
				colons++;
			} else if (Character.digit(b, 16) == -1) {
				return false;
			}
		}

		if (dots < 4) {
			if (colons > 0 && colons < 8) {
				return checkIpv6(bytes, 0, bytes.length);
			}
			return checkIpv4(bytes, 0, bytes.length);
		}
		return false;
	}

	/*
	 *  Checks only dot decimal format(192.168.0.208 for example)
	 *  more -> https://en.wikipedia.org/wiki/IPv4
	 */
	private static boolean checkIpv4(byte[] bytes, int pos, int length) {
		int start = pos;
		for (int i = pos; i < length; i++) {
			// assume at least one more symbol is present after dot
			if (i == length - 1 && bytes[i] == '.') {
				return false;
			}
			if (bytes[i] == '.' || i == length - 1) {
				int v;
				if (i - start == 0 && i != length - 1) {
					return false;
				}
				try {
					v = decodeUnsignedInt(bytes, start, i - start);
				} catch (ParseException e) {
					return false;
				}
				if (v < 0 || v > 255) return false;
				start = i + 1;
			}
		}
		return true;
	}

	/*
	 *   http://stackoverflow.com/questions/5963199/ipv6-validation
	 *   rfc4291
	 *
	 *   IPV6 addresses are represented as 8, 4 hex digit groups of numbers
	 *   2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d
	 *
	 *   leading zeros are not necessary, however at least one digit should be present
	 *
	 *   the null group ':0000:0000:0000'(one or more) could be substituted with '::' once per address
	 *
	 *   x:x:x:x:x:x:d.d.d.d - 6 ipv6 + 4 ipv4
	 *   ::d.d.d.d
	 * */
	private static boolean checkIpv6(byte[] bytes, int pos, int length) {
		boolean shortHand = false;  // denotes usage of ::
		int numCount = 0;
		int blocksCount = 0;
		int start = 0;
		while (pos < length) {
			if (bytes[pos] == ':') {
				start = pos;
				blocksCount++;
				numCount = 0;
				if (pos > 0 && bytes[pos - 1] == ':') {
					if (shortHand) return false;
					else {
						shortHand = true;
					}
				}
			} else if (bytes[pos] == '.') {
				return checkIpv4(bytes, start + 1, length - start + 1);
			} else {
				if (Character.digit(bytes[pos], 16) == -1) {
					return false;
				}
				numCount++;
				if (numCount > 4) {
					return false;
				}
			}
			pos++;
		}
		return blocksCount > 6 || shortHand;
	}

	static int skipSpaces(byte[] bytes, int pos, int end) {
		while (pos < end && bytes[pos] == ' ') {
			pos++;
		}
		return pos;
	}

	static int parseQ(byte[] bytes, int pos, int length) throws ParseException {
		if (bytes[pos] == '1') {
			return 100;
		} else if (bytes[pos] == '0') {
			if (length == 1) return  0;
			length = length > 4 ? 2 : length - 2;
			int q = decodeUnsignedInt(bytes, pos + 2, length);
			if (length == 1) q *= 10;
			return q;
		}
		throw INVALID_Q_VALUE;
	}

	/**
	 * Method which creates string with parameters and its value in format URL. Using encoding UTF-8
	 *
	 * @param q map in which keys if name of parameters, value - value of parameters.
	 * @return string with parameters and its value in format URL
	 */
	public static String renderQueryString(Map<String, String> q) {
		return renderQueryString(q, ENCODING);
	}

	/**
	 * Method which creates string with parameters and its value in format URL
	 *
	 * @param q   map in which keys if name of parameters, value - value of parameters.
	 * @param enc encoding of this string
	 * @return string with parameters and its value in format URL
	 */
	public static String renderQueryString(Map<String, String> q, String enc) {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> e : q.entrySet()) {
			String name = encode(e.getKey(), enc);
			sb.append(name);
			if (e.getValue() != null) {
				sb.append('=');
				sb.append(encode(e.getValue(), enc));
			}
			sb.append('&');
		}
		if (sb.length() > 0)
			sb.setLength(sb.length() - 1);
		return sb.toString();
	}

	/**
	 * Translates a string into application/x-www-form-urlencoded format using a specific encoding scheme.
	 * This method uses the supplied encoding scheme to obtain the bytes for unsafe characters
	 *
	 * @param s   string for encoding
	 * @param enc new encoding
	 * @return the translated String.
	 */
	static String encode(String s, String enc) {
		try {
			return URLEncoder.encode(s, enc);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("Can't encode with supplied encoding: " + enc, e);
		}
	}

	/**
	 * Encodes non-negative decimal value into bytes and puts these bytes into an array starting from specified position.
	 *
	 * @param array array that will hold encoded bytes
	 * @param pos   position from which encoded bytes will be put into array
	 * @param value non-negative decimal value to be encoded
	 * @return number of bytes that were put into array
	 * @throws ParseException if value to be encoded is negative
	 */
	public static int encodeUnsignedDecimal(byte[] array, int pos, long value) throws ParseException {
		if (value < 0) {
			throw ENCODE_NEGATIVE_VALUE;
		}
		return encodeDecimal(array, pos, value);
	}

	/**
	 * Decodes non-negative {@code int} value from byte array starting from specified position, with given length.
	 *
	 * @param array array that stores bytes to be decoded
	 * @param pos   position from which to start decoding
	 * @param len   number of bytes to be decoded
	 * @return {@code int} value of number that has been encoded
	 * @throws ParseException in case decoded value is out of bounds of type {@code int}, is negative, or in case
	 *                        decimal value has been inproperly encoded
	 */
	public static int decodeUnsignedInt(byte[] array, int pos, int len) throws ParseException {
		int offsetLeft = trimOffsetLeft(array, pos, len);
		if (array[pos + offsetLeft] == (byte) '-') {
			throw DECODE_NEGATIVE_VALUE;
		}

		pos += offsetLeft;
		len -= offsetLeft;
		len -= trimOffsetRight(array, pos, len);
		return decodeInt(array, pos, len);
	}

	/**
	 * Decodes non-negative {@code long} value from byte array starting from specified position, with given length
	 *
	 * @param array array that stores bytes to be decoded
	 * @param pos   position from which to start decoding
	 * @param len   number of bytes to be decoded
	 * @return {@code long} value of number that has been encoded
	 * @throws ParseException in case decoded value is out of bounds of type {@code long}, is negative, or in case
	 *                        decimal value has been inproperly encoded
	 */
	public static long decodeUnsignedLong(byte[] array, int pos, int len) throws ParseException {
		int offsetLeft = trimOffsetLeft(array, pos, len);
		if (array[pos + offsetLeft] == (byte) '-') {
			throw DECODE_NEGATIVE_VALUE;
		}

		pos += offsetLeft;
		len -= offsetLeft;
		len -= trimOffsetRight(array, pos, len);

		return decodeLong(array, pos, len);
	}

	private static int trimOffsetLeft(byte[] array, int pos, int len) {
		for (int i = 0; i < len; i++) {
			if (array[pos + i] != SP && array[pos + i] != HT) {
				return i;
			}
		}
		return 0;
	}

	private static int trimOffsetRight(byte[] array, int pos, int len) {
		for (int i = len - 1; i >= 0; i--) {
			if (array[pos + i] != SP && array[pos + i] != HT) {
				return len - i - 1;
			}
		}
		return 0;
	}
}
