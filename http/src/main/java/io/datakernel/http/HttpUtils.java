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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.net.InetAddresses;
import io.datakernel.annotation.Nullable;

import java.io.UnsupportedEncodingException;
import java.net.HttpCookie;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Util for working with {@link HttpRequest}
 */
public final class HttpUtils {
	public static class Pair<E> {
		private E value;
		private double quot;

		public Pair(E value) {
			this.value = value;
		}

		public Pair(E value, double quot) {
			this.value = value;
			this.quot = quot;
		}

		public E getValue() {
			return value;
		}

		public void setValue(E value) {
			this.value = value;
		}

		public double getQuot() {
			return quot;
		}

		public void setQuot(double quot) {
			this.quot = quot;
		}
	}

	private static final Splitter splitCookies = Splitter.on("; ");
	private static final Splitter splitComma = Splitter.on(',').trimResults();
	private static final Splitter querySplitter = Splitter.on('&');
	private static final String ENCODING = "UTF-8";

	private static SimpleDateFormat createCookieDateFormat() {
		SimpleDateFormat df = new SimpleDateFormat("EEE',' dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);
		df.setTimeZone(TimeZone.getTimeZone("GMT"));
		return df;
	}

	public static int skipSpaces(byte[] bytes, int pos, int end) {
		while (pos < end && bytes[pos] == ' ') {
			pos++;
		}
		return pos;
	}

	/**
	 * Returns a real IP of client which send this request, if it has header X_FORWARDED_FOR
	 *
	 * @param request received request
	 */
	public static InetAddress getRealIp(HttpRequest request) {
		String s = request.getHeaderString(HttpHeader.X_FORWARDED_FOR);
		if (!isNullOrEmpty(s)) {
			String clientIP = splitComma.split(s).iterator().next();
			try {
				return InetAddresses.forString(clientIP);
			} catch (IllegalArgumentException ignored) {
			}
		}
		return request.getRemoteAddress();
	}

	public static InetAddress getRealIpNginx(HttpRequest request) {
		String s = request.getHeaderString(HttpHeader.X_REAL_IP);
		if (!isNullOrEmpty(s))
			return InetAddresses.forString(s.trim());

		return getRealIp(request);
	}

	/**
	 * Returns the collection of cookies from header value
	 *
	 * @param headerValue string with COOKIE - header value
	 * @return the list of cookies
	 */
	public static List<HttpCookie> parseClientCookies(@Nullable String headerValue) {
		List<HttpCookie> cookies = Lists.newArrayList();
		if (headerValue == null)
			return cookies;
		for (String s : splitCookies.split(headerValue)) {
			int pos = s.indexOf('=');
			if (pos < 0) {
				continue;
			}
			String name = s.substring(0, pos);
			String value = s.substring(pos + 1);
			HttpCookie cookie = new HttpCookie(name, value);
			cookie.setVersion(0);
			cookies.add(cookie);
		}
		return cookies;
	}

	/**
	 * Returns the collection of cookies from Http request
	 *
	 * @param request request with COOKIE - header value
	 * @return the list of cookies
	 */
	public static List<HttpCookie> getCookies(HttpRequest request) {
		String header = request.getHeaderString(HttpHeader.COOKIE);
		return parseClientCookies(header);
	}

	/**
	 * Returns the cookie from request with name from argument.
	 *
	 * @param request request with COOKIE - header value
	 * @param name    name of cookie to be returned
	 */
	public static HttpCookie getCookie(HttpRequest request, String name) {
		List<HttpCookie> cookies = getCookies(request);
		for (HttpCookie cookie : cookies) {
			if (name.equals(cookie.getName()))
				return cookie;
		}
		return null;
	}

	/**
	 * Checks if the cookie with cookieName has the cookieValue
	 *
	 * @param request     request with COOKIE - header value
	 * @param cookieName  name of cookie
	 * @param cookieValue expected value of this cookie
	 * @return true if the name matches the value, false else
	 */
	public static boolean checkCookie(HttpRequest request, String cookieName, String cookieValue) {
		HttpCookie cookie = getCookie(request, cookieName);
		if (cookie == null)
			return false;
		return cookie.getValue().equals(cookieValue);
	}

	public static String cookiesToString(Collection<HttpCookie> cookies) {
		StringBuilder sb = new StringBuilder();
		Iterator<HttpCookie> iter = cookies.iterator();
		if (!iter.hasNext())
			return "";
		sb.append(iter.next().toString());
		while (iter.hasNext()) {
			sb.append("; ").append(iter.next().toString());
		}
		return sb.toString();
	}

	public static String cookieToServerString(HttpCookie cookie) {
		StringBuilder sb = new StringBuilder();
		cookieToServerString(sb, cookie);
		return sb.toString();
	}

	private static void cookieToServerString(StringBuilder sb, HttpCookie cookie) {
		sb.append(cookie.getName());
		sb.append('=');
		sb.append(cookie.getValue());
		String path = cookie.getPath();
		if ((path == null) || path.isEmpty()) {
			path = "/";
		}
		sb.append(";Path=");
		sb.append(path);
		if (cookie.getMaxAge() >= 0) {
			long tm = cookie.getMaxAge() > 0 ? System.currentTimeMillis() : 0;
			Date dt = new Date(tm + (1000 * cookie.getMaxAge()));
			sb.append(";Expires=");
			SimpleDateFormat cookieDateFormat = createCookieDateFormat();
			sb.append(cookieDateFormat.format(dt));
		}
	}

	/**
	 * Returns  new cookie
	 *
	 * @param name   specifying name of the cookie
	 * @param value  specifying value of the cookie
	 * @param expiry specifying the maximum age of the cookie in seconds; if negative, means the cookie
	 *               persists until browser shutdown
	 * @param path   specifying a path that contains a servlet name
	 */
	public static HttpCookie newCookie(String name, String value, int expiry, String path) {
		HttpCookie cookie = new HttpCookie(name, value);
		cookie.setVersion(0);
		cookie.setMaxAge(expiry);
		cookie.setPath(path);
		return cookie;
	}

	/**
	 * Returns new cookie with servlet name "/"
	 *
	 * @param name   specifying name of the cookie
	 * @param value  specifying value of the cookie
	 * @param expiry specifying the maximum age of the cookie in seconds; if negative, means the cookie
	 *               persists until browser shutdown
	 */
	public static HttpCookie newCookie(String name, String value, int expiry) {
		return newCookie(name, value, expiry, "/");
	}

	/**
	 * Returns new cookie with servlet name "/" which will persists until browser shutdown
	 *
	 * @param name  specifying the name of the cookie
	 * @param value specifying the value of the cookie
	 */
	public static HttpCookie newCookie(String name, String value) {
		return newCookie(name, value, -1);
	}

	/**
	 * Returns  the host of Http request
	 *
	 * @param request Http request with header host
	 */
	public static String getHost(HttpRequest request) {
		String host = request.getHeaderString(HttpHeader.HOST);
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
		for (String pair : querySplitter.split(query)) {
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
	 * Method which creates string with parameters and its value in format URL
	 *
	 * @param q   multimap in which keys if name of parameters, value - value of parameters.
	 * @param enc encoding of this string
	 * @return string with parameters and its value in format URL
	 */
	public static String urlQueryString(Multimap<String, String> q, String enc) {
		StringBuilder sb = new StringBuilder();
		for (String key : q.keySet()) {
			Collection<String> values = q.get(key);
			for (String value : values) {
				sb.append(encode(key, enc));
				if (value != null) {
					sb.append('=');
					sb.append(encode(value, enc));
				}
				sb.append('&');
			}
		}
		if (sb.length() > 0)
			sb.setLength(sb.length() - 1);
		return sb.toString();
	}

	/**
	 * Method which creates string with parameters and its value in format URL. Using encoding UTF-8
	 *
	 * @param q multimap in which keys if name of parameters, value - value of parameters.
	 * @return string with parameters and its value in format URL
	 */
	public static String urlQueryString(Multimap<String, String> q) {
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
