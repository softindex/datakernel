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

package io.datakernel.http;

import io.datakernel.exception.ParseException;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.datakernel.bytebuf.ByteBufStrings.decodeDecimal;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.util.StringUtils.splitToList;

/**
 * Util for working with {@link HttpRequest}
 */
public final class HttpUtils {
	private static final char COMMA_SEPARATOR = ',';
	private static final char QUERY_SEPARATOR = '&';
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
					v = decodeDecimal(bytes, start, i - start);
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
		} else {
			length = length > 4 ? 2 : length - 2;
			int q = decodeDecimal(bytes, pos + 2, length);
			if (length == 1) q *= 10;
			return q;
		}
	}

	/**
	 * Returns a real IP of client which send this request, if it has header X_FORWARDED_FOR
	 *
	 * @param request received request
	 */
	public static InetAddress getRealIp(HttpRequest request) {
		String s = request.getHeader(HttpHeaders.X_FORWARDED_FOR);
		if (!isNullOrEmpty(s)) {
			String clientIP = splitToList(COMMA_SEPARATOR, s).iterator().next().trim();
			try {
				return HttpUtils.inetAddress(clientIP);
			} catch (Exception ignored) {
			}
		}
		return request.getRemoteAddress();
	}

	public static InetAddress getRealIpNginx(HttpRequest request) throws ParseException {
		String s = request.getHeader(HttpHeaders.X_REAL_IP);
		if (!isNullOrEmpty(s)) {
			try {
				return InetAddress.getByName(s.trim());
			} catch (Exception ignored) {
			}
		}
		return getRealIp(request);
	}

	/**
	 * Returns  the host of Http request
	 *
	 * @param request Http request with header host
	 */
	public static String getHost(HttpRequest request) throws ParseException {
		String host = request.getHeader(HttpHeaders.HOST);
		if ((host == null) || host.isEmpty())
			throw new ParseException("Absent header host in " + request);
		return host;
	}

	/**
	 * Returns the URL from Http Request
	 *
	 * @param request Http request with  URL
	 */
	public static String getFullUrl(HttpRequest request) throws ParseException {
		HttpUri url = request.getUrl();
		if (!url.isPartial()) {
			return url.toString();
		}
		return "http://" + getHost(request) + url.getPathAndQuery();
	}

	/**
	 * Method which  parses string with URL of query, and returns collection with keys - name of
	 * parameter, value - value of it. Using encoding UTF-8
	 *
	 * @param query string with URL for parsing
	 * @return collection with keys - name of parameter, value - value of it.
	 */
	public static Map<String, String> extractParameters(String query) throws ParseException {
		return extractParameters(query, ENCODING);
	}

	/**
	 * Method which  parses string with URL of query, and returns collection with keys - name of
	 * parameter, value - value of it.
	 *
	 * @param query string with URL for parsing
	 * @param enc   encoding of this string
	 * @return collection with keys - name of parameter, value - value of it.
	 */
	public static Map<String, String> extractParameters(String query, String enc) throws ParseException {
		LinkedHashMap<String, String> qps = new LinkedHashMap<>();
		for (String pair : splitToList(QUERY_SEPARATOR, query)) {
			pair = pair.trim();
			int pos = pair.indexOf('=');
			String name;
			String val = null;
			if (pos < 0)
				name = pair;
			else {
				name = pos == 0 ? "" : pair.substring(0, pos);
				++pos;
				val = pos < pair.length() ? pair.substring(pos) : "";
			}
			name = decode(name, enc);
			if (val != null)
				val = decode(val, enc);
			qps.put(name, val);
		}
		return qps;
	}

	/**
	 * Method which creates string with parameters and its value in format URL. Using encoding UTF-8
	 *
	 * @param q map in which keys if name of parameters, value - value of parameters.
	 * @return string with parameters and its value in format URL
	 */
	public static String urlQueryString(Map<String, String> q) {
		return urlQueryString(q, ENCODING);
	}

	/**
	 * Method which creates string with parameters and its value in format URL
	 *
	 * @param q   map in which keys if name of parameters, value - value of parameters.
	 * @param enc encoding of this string
	 * @return string with parameters and its value in format URL
	 */
	public static String urlQueryString(Map<String, String> q, String enc) {
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
	public static String encode(String s, String enc) {
		try {
			return URLEncoder.encode(s, enc);
		} catch (UnsupportedEncodingException e) {
			throw new AssertionError("Can't encode with supplied encoding: " + enc, e);
		}
	}

	/**
	 * Decodes a application/x-www-form-urlencoded string using a specific encoding scheme. The supplied
	 * encoding is used to determine what characters are represented by any consecutive sequences of the
	 * form "%xy".
	 *
	 * @param s   string for decoding
	 * @param enc the name of a supported character encoding
	 * @return the newly decoded String
	 */
	public static String decode(String s, String enc) throws ParseException {
		try {
			return URLDecoder.decode(s, enc);
		} catch (RuntimeException e) {
			throw new ParseException("Can't decode", e);
		} catch (UnsupportedEncodingException e) {
			throw new AssertionError("Can't decode with supplied encoding: " + enc, e);
		}
	}

	static boolean isNullOrEmpty(String s) {
		return s == null || s.isEmpty();
	}

	static String nullToEmpty(String string) {
		return string == null ? "" : string;
	}
}
