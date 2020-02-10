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

import io.datakernel.common.parse.ParseException;
import io.datakernel.common.parse.UnknownFormatException;
import org.jetbrains.annotations.Nullable;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.Map;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.HttpHeaders.HOST;

/**
 * Util for working with {@link HttpRequest}
 */
public final class HttpUtils {
	public static final ParseException INVALID_Q_VALUE = new ParseException("Value of 'q' should start either from 0 or 1");
	private static final int URI_DEFAULT_CAPACITY = 1 << 5;

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
	 * Checks only for a dot decimal format (192.168.0.208 for example) more -> https://en.wikipedia.org/wiki/IPv4
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
					v = trimAndDecodePositiveInt(bytes, start, i - start);
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

	public static int skipSpaces(byte[] bytes, int pos, int end) {
		while (pos < end && bytes[pos] == ' ') {
			pos++;
		}
		return pos;
	}

	public static int parseQ(byte[] bytes, int pos, int length) throws ParseException {
		if (bytes[pos] == '1') {
			return 100;
		} else if (bytes[pos] == '0') {
			if (length == 1) return 0;
			length = length > 4 ? 2 : length - 2;
			int q = trimAndDecodePositiveInt(bytes, pos + 2, length);
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
		return renderQueryString(q, "UTF-8");
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
			String name = urlEncode(e.getKey(), enc);
			sb.append(name);
			if (e.getValue() != null) {
				sb.append('=');
				sb.append(urlEncode(e.getValue(), enc));
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
	 * @param string string for encoding
	 * @param enc    new encoding
	 * @return the translated String.
	 */
	public static String urlEncode(String string, String enc) {
		try {
			return URLEncoder.encode(string, enc);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("Can't encode with supplied encoding: " + enc, e);
		}
	}

	public static String urlDecode(@Nullable String string, String enc) throws ParseException {
		if (string == null) {
			throw new ParseException(HttpUtils.class, "No string to decode");
		}
		try {
			return URLDecoder.decode(string, enc);
		} catch (UnsupportedEncodingException e) {
			throw new UnknownFormatException(HttpUtils.class, "Can't encode with supplied encoding: " + enc, e);
		}
	}

	public static int trimAndDecodePositiveInt(byte[] array, int pos, int len) throws ParseException {
		int left = trimLeft(array, pos, len);
		pos += left;
		len -= left;
		len -= trimRight(array, pos, len);
		return decodePositiveInt(array, pos, len);
	}

	private static int trimLeft(byte[] array, int pos, int len) {
		for (int i = 0; i < len; i++) {
			if (array[pos + i] != SP && array[pos + i] != HT) {
				return i;
			}
		}
		return 0;
	}

	private static int trimRight(byte[] array, int pos, int len) {
		for (int i = len - 1; i >= 0; i--) {
			if (array[pos + i] != SP && array[pos + i] != HT) {
				return len - i - 1;
			}
		}
		return 0;
	}

	/**
	 * (RFC3986) scheme://authority/path/?query#fragment
	 */
	@Nullable
	public static String getFullUri(HttpRequest request, int builderCapacity) {
		String host = request.getHeader(HOST);
		if (host == null) {
			return null;
		}
		String query = request.getQuery();
		String fragment = request.getFragment();
		StringBuilder fullUriBuilder = new StringBuilder(builderCapacity)
				.append(request.isHttps() ? "https://" : "http://")
				.append(host)
				.append(request.getPath());
		if (!query.isEmpty()) {
			fullUriBuilder.append("?").append(query);
		}
		if (!fragment.isEmpty()) {
			fullUriBuilder.append("#").append(fragment);
		}
		return fullUriBuilder.toString();
	}

	@Nullable
	public static String getFullUri(HttpRequest request) {
		return getFullUri(request, URI_DEFAULT_CAPACITY);
	}

	/**
	 * RFC-7231, sections 6.5 and 6.6
	 */
	public static String getHttpErrorTitle(int code) {
		switch (code) {
			case 400:
				return "400. Bad Request";
			case 402:
				return "402. Payment Required";
			case 403:
				return "403. Forbidden";
			case 404:
				return "404. Not Found";
			case 405:
				return "405. Method Not Allowed";
			case 406:
				return "406. Not Acceptable";
			case 408:
				return "408. Request Timeout";
			case 409:
				return "409. Conflict";
			case 410:
				return "410. Gone";
			case 411:
				return "411. Length Required";
			case 413:
				return "413. Payload Too Large";
			case 414:
				return "414. URI Too Long";
			case 415:
				return "415. Unsupported Media Type";
			case 417:
				return "417. Expectation Failed";
			case 426:
				return "426. Upgrade Required";
			case 500:
				return "500. Internal Server Error";
			case 501:
				return "501. Not Implemented";
			case 502:
				return "502. Bad Gateway";
			case 503:
				return "503. Service Unavailable";
			case 504:
				return "504. Gateway Timeout";
			case 505:
				return "505. HTTP Version Not Supported";
			default:
				return code + ". Unknown HTTP code, returned from an error";
		}
	}
}
