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

import io.datakernel.util.Splitter;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static io.datakernel.util.ByteBufStrings.*;

/**
 * Util for working with {@link HttpRequest}
 */
public final class HttpUtils {
	private static final Splitter commaSplitter = Splitter.on(',').trimResults();
	private static final Splitter querySplitter = Splitter.on('&').trimResults().omitEmptyStrings();
	private static final String ENCODING = "UTF-8";

	private static Pattern VALID_IPV4_PATTERN =
			Pattern.compile("^(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)(\\.(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)){3}$", Pattern.CASE_INSENSITIVE);
	private static Pattern VALID_IPV6_PATTERN =
			Pattern.compile("^(?:[0-9a-fA-F]{0,4}:){0,7}[0-9a-fA-F]{0,4}$", Pattern.CASE_INSENSITIVE);

	public static InetAddress inetAddress(String host) {
		try {
			return InetAddress.getByName(host);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	// https://url.spec.whatwg.org/
	public static boolean isInetAddress(String host) {
		int colons = 0;
		int dots = 0;
		byte[] bytes = encodeAscii(host);

		if (bytes[0] == '[' && bytes[bytes.length - 1] == ']') {
			return checkIpv6(bytes);
		}

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

		if (colons > 0 && colons < 17) {
			if (dots > 0) {
				// not implemented
			}
			return checkIpv6(bytes);
		} else if (dots > 0 && dots < 5) {
			return checkIpv4(bytes);
		}
		return false;
	}

	/*
	 *  Checks only xxx.xxx.xxx.xxx format
	 *  more - https://ru.wikipedia.org/wiki/IPv4
	 */
	public static boolean checkIpv4(byte[] bytes) {
		int start = 0;
		for (int i = 0; i < bytes.length; i++) {
			if (bytes[i] == '.' || i == bytes.length - 1) {
				int v = decodeDecimal(bytes, start, i - start);
				if (v < 0 || v > 255) return false;
				start = i + 1;
			}
		}
		return true;
	}

	/*
	* http://stackoverflow.com/questions/5963199/ipv6-validation
	* rfc2732
	*/
	public static boolean checkIpv6(byte[] bytes) {
		// TODO (arashev) remove later
		return VALID_IPV6_PATTERN.matcher(decodeAscii(bytes)).matches();
	}

	public static int skipSpaces(byte[] bytes, int pos, int end) {
		while (pos < end && bytes[pos] == ' ') {
			pos++;
		}
		return pos;
	}

	public static int parseQ(byte[] bytes, int pos, int length) {
		if (bytes[pos] == '1') {
			return 100;
		} else {
			length = length > 4 ? 2 : length - 2;
			int q = decodeDecimal(bytes, pos + 2, length);
			if (length == 1) q *= 10;
			return q;
		}
	}

	public static String nullToEmpty(String string) {
		return string == null ? "" : string;
	}

	/**
	 * Returns a real IP of client which send this request, if it has header X_FORWARDED_FOR
	 *
	 * @param request received request
	 */
	public static InetAddress getRealIp(HttpRequest request) {
		String s = request.getHeader(HttpHeaders.X_FORWARDED_FOR);
		if (!isNullOrEmpty(s)) {
			String clientIP = commaSplitter.splitToList(s).iterator().next();
			try {
				return HttpUtils.inetAddress(clientIP);
			} catch (IllegalArgumentException ignored) {
			}
		}
		return request.getRemoteAddress();
	}

	public static boolean isNullOrEmpty(String s) {
		return s == null || s.isEmpty();
	}

	public static InetAddress getRealIpNginx(HttpRequest request) throws UnknownHostException {
		String s = request.getHeader(HttpHeaders.X_REAL_IP);
		if (!isNullOrEmpty(s))
			return InetAddress.getByName(s.trim());

		return getRealIp(request);
	}

	/**
	 * Returns  the host of Http request
	 *
	 * @param request Http request with header host
	 */
	public static String getHost(HttpRequest request) {
		String host = request.getHeader(HttpHeaders.HOST);
		if ((host == null) || host.isEmpty())
			throw new IllegalArgumentException("Absent header host in " + request);
		return host;
	}

	/**
	 * Returns the URL from Http Request
	 *
	 * @param request Http request with  URL
	 */
	public static String getFullUrl(HttpRequest request) {
		HttpUri url = request.getUrl();
		if (!url.isPartial())
			return url.toString();
		return "http://" + getHost(request) + url.getPathAndQuery();
	}

	/**
	 * Method which  parses string with URL of query, and returns collection with keys - name of
	 * parameter, value - value of it.
	 *
	 * @param query string with URL for parsing
	 * @param enc   encoding of this string
	 * @return collection with keys - name of parameter, value - value of it.
	 */
	public static Map<String, String> parse(String query, String enc) {
		LinkedHashMap<String, String> qps = new LinkedHashMap<>();
		for (String pair : querySplitter.splitToList(query)) {
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
	 * Method which  parses string with URL of query, and returns collection with keys - name of
	 * parameter, value - value of it. Using encoding UTF-8
	 *
	 * @param query string with URL for parsing
	 * @return collection with keys - name of parameter, value - value of it.
	 */
	public static Map<String, String> parse(String query) {
		return parse(query, ENCODING);
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
	 * Method which creates string with parameters and its value in format URL. Using encoding UTF-8
	 *
	 * @param q map in which keys if name of parameters, value - value of parameters.
	 * @return string with parameters and its value in format URL
	 */
	public static String urlQueryString(Map<String, String> q) {
		return urlQueryString(q, ENCODING);
	}

	/**
	 * Translates a string into application/x-www-form-urlencoded format using a specific encoding scheme.
	 * This method uses the supplied encoding scheme to obtain the bytes for unsafe characters
	 *
	 * @param s   string for encoding
	 * @param enc new encoding
	 * @return the translated String.
	 */
	private static String encode(String s, String enc) {
		try {
			return URLEncoder.encode(s, enc);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
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
	private static String decode(String s, String enc) {
		try {
			return URLDecoder.decode(s, enc);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
}
