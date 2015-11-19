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

import io.datakernel.util.ByteBufStrings;

import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class HttpCookie {
	private abstract static class CookieHandler {
		protected abstract void handle(HttpCookie cookie, byte[] bytes, int start, int end);
	}

	private static final Charset ISO = Charset.forName("ISO-8859-1");

	private static final byte[] EXPIRES = "expires".getBytes(ISO);
	private static final byte[] MAX_AGE = "max-age".getBytes(ISO);
	private static final byte[] DOMAIN = "domain".getBytes(ISO);
	private static final byte[] PATH = "path".getBytes(ISO);
	private static final byte[] HTTPONLY = "httponly".getBytes(ISO);
	private static final byte[] SECURE = "secure".getBytes(ISO);

	private static Map<Integer, CookieHandler> handlers = new HashMap<>();

	static {
		handlers.put(hash(EXPIRES), new CookieHandler() {
			@Override
			protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
				// TODO
				System.out.println(new String(bytes, start, end - start));
				cookie.setExpirationDate(new Date());
			}
		});

		handlers.put(hash(MAX_AGE), new CookieHandler() {
			@Override
			protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
				System.out.println(new String(bytes, start, end - start));
				cookie.setMaxAge(ByteBufStrings.decodeDecimal(bytes, start, end - start));
			}
		});

		handlers.put(hash(DOMAIN), new CookieHandler() {
			@Override
			protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
				System.out.println(new String(bytes, start, end - start));
				cookie.setDomain(new String(bytes, start, end - start));
			}
		});

		handlers.put(hash(PATH), new CookieHandler() {
			@Override
			protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
				System.out.println(new String(bytes, start, end - start));
				cookie.setPath(new String(bytes, start, end - start));
			}
		});

		handlers.put(hash(HTTPONLY), new CookieHandler() {
			@Override
			protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
				System.out.println(new String(bytes, start, end - start));
				cookie.setHttpOnly(true);
			}
		});

		handlers.put(hash(SECURE), new CookieHandler() {
			@Override
			protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
				System.out.println(new String(bytes, start, end - start));
				cookie.setSecure(true);
			}
		});
	}

	public static int hash(byte[] bytes, int start, int end) {
		int hash = 0;
		for (int i = start; i < end; i++) {
			byte value = bytes[i];
			if (value > 64 && value < 91) {
				value = (byte) (value + ('A' - 'a'));
			}
			hash += 31 * hash + value;
		}
		return hash;
	}

	private static int hash(byte[] bytes) {
		return hash(bytes, 0, bytes.length);
	}

	private final String name;
	private final String value;
	private Date expirationDate;
	private int maxAge;
	private String domain;
	private String path;
	private boolean secure;
	private boolean httpOnly;
	private String extension;

	public static List<HttpCookie> parse(String cookieString) {
		List<HttpCookie> cookies = new ArrayList<>();
		byte[] bytes = cookieString.getBytes(ISO);
		parse(cookies, bytes);
		return cookies;
	}

	private static void parse(List<HttpCookie> cookies, byte[] bytes) {
		int pos = 0;
		HttpCookie cookie = new HttpCookie("", "");
		while (pos < bytes.length) {
			int keyStart = pos;
			while (bytes[pos] != ';') {
				pos++;
			}
			int valueEnd = pos;
			int equalSign = -1;
			for (int i = keyStart; i < valueEnd; i++) {
				if (bytes[i] == '=') {
					equalSign = i;
					break;
				}
			}

			String kkey;
			String vvalue;

			if (equalSign != -1) {
				kkey = new String(bytes, keyStart, equalSign - keyStart, ISO);
				vvalue = new String(bytes, equalSign + 1, valueEnd - equalSign - 1, ISO);
			} else {
				kkey = "";
				vvalue = new String(bytes, keyStart, valueEnd - keyStart, ISO);
			}

			//hash(MAX_AGE, 0, MAX_AGE.length)

			CookieHandler handler = handlers.get(hash(bytes, keyStart, equalSign == -1 ? valueEnd : equalSign));
			if (equalSign == -1 && handler == null) {
				cookie.setExtension(new String(bytes, keyStart, valueEnd - keyStart, ISO));
			} else if (handler == null) {
				String key = new String(bytes, keyStart, equalSign - keyStart, ISO);
				String value = new String(bytes, equalSign + 1, valueEnd - equalSign - 1, ISO);
				cookie = new HttpCookie(key, value);
				cookies.add(cookie);
			} else {
				handler.handle(cookie, bytes, equalSign + 1, valueEnd);
			}

			pos = valueEnd + 2;
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(name).append("=").append(value).append("; ");
		if (expirationDate != null) {
			renderExpirationDate(sb);
		}
		if (maxAge != 0) {
			sb.append("Max-Age=").append(maxAge).append("; ");
		}
		if (domain != null) {
			sb.append("Domain=").append(domain).append("; ");
		}
		if (path != null) {
			sb.append("Path=").append(path).append("; ");
		}
		if (secure) {
			sb.append("Secure; ");
		}
		if (httpOnly) {
			sb.append("HttpOnly; ");
		}
		if (extension != null) {
			sb.append(extension).append("; ");
		}
		return sb.toString();
	}

	private void renderExpirationDate(StringBuilder sb) {
		DateFormat df = new SimpleDateFormat("EEE, dd-MMM-yyyy HH:mm:ss zzz");
		df.setTimeZone(TimeZone.getTimeZone("GMT"));
		String cookieExpire = df.format(expirationDate);
		sb.append("Expires=").append(cookieExpire).append("; ");
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public String getName() {
		return name;
	}

	public HttpCookie(String name, String value) {
		this.name = name;
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public Date getExpirationDate() {
		return expirationDate;
	}

	public void setExpirationDate(Date expirationDate) {
		this.expirationDate = expirationDate;
	}

	public int getMaxAge() {
		return maxAge;
	}

	public void setMaxAge(int maxAge) {
		this.maxAge = maxAge;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public boolean isSecure() {
		return secure;
	}

	public void setSecure(boolean secure) {
		this.secure = secure;
	}

	public boolean isHttpOnly() {
		return httpOnly;
	}

	public void setHttpOnly(boolean httpOnly) {
		this.httpOnly = httpOnly;
	}

	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}
}
