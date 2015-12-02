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

import io.datakernel.bytebuf.ByteBuf;

import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;

import static io.datakernel.util.ByteBufStrings.*;

@SuppressWarnings("unused")
public class HttpHeader {
	private static final int HEADERS_SLOTS = 512;
	private static final int MAX_PROBINGS = 2;

	private static final HttpHeader[] HEADERS = new HttpHeader[HEADERS_SLOTS];

	public static final HttpHeader CACHE_CONTROL = register("Cache-Control");
	public static final HttpHeader CONTENT_LENGTH = register("Content-Length");
	public static final HttpHeader CONTENT_TYPE = register("Content-Type");
	public static final HttpHeader DATE = register("Date");
	public static final HttpHeader PRAGMA = register("Pragma");
	public static final HttpHeader VIA = register("Via");
	public static final HttpHeader WARNING = register("Warning");
	public static final HttpHeader ACCEPT = register("Accept");
	public static final HttpHeader ACCEPT_CHARSET = register("Accept-Charset");
	public static final HttpHeader ACCEPT_ENCODING = register("Accept-Encoding");
	public static final HttpHeader ACCEPT_LANGUAGE = register("Accept-Language");
	public static final HttpHeader ACCESS_CONTROL_REQUEST_HEADERS = register("Access-Control-Request-Headers");
	public static final HttpHeader ACCESS_CONTROL_REQUEST_METHOD = register("Access-Control-Request-Method");
	public static final HttpHeader AUTHORIZATION = register("Authorization");
	public static final HttpHeader CONNECTION = register("Connection");
	public static final HttpHeader COOKIE = register("Cookie");
	public static final HttpHeader EXPECT = register("Expect");
	public static final HttpHeader FROM = register("From");
	public static final HttpHeader FOLLOW_ONLY_WHEN_PRERENDER_SHOWN = register("Follow-Only-When-Prerender-Shown");
	public static final HttpHeader HOST = register("Host");
	public static final HttpHeader IF_MATCH = register("If-Match");
	public static final HttpHeader IF_MODIFIED_SINCE = register("If-Modified-Since");
	public static final HttpHeader IF_NONE_MATCH = register("If-None-Match");
	public static final HttpHeader IF_RANGE = register("If-Range");
	public static final HttpHeader IF_UNMODIFIED_SINCE = register("If-Unmodified-Since");
	public static final HttpHeader LAST_EVENT_ID = register("Last-Event-ID");
	public static final HttpHeader MAX_FORWARDS = register("Max-Forwards");
	public static final HttpHeader ORIGIN = register("Origin");
	public static final HttpHeader PROXY_AUTHORIZATION = register("Proxy-Authorization");
	public static final HttpHeader RANGE = register("Range");
	public static final HttpHeader REFERER = register("Referer");
	public static final HttpHeader TE = register("TE");
	public static final HttpHeader UPGRADE = register("Upgrade");
	public static final HttpHeader USER_AGENT = register("User-Agent");
	public static final HttpHeader ACCEPT_RANGES = register("Accept-Ranges");
	public static final HttpHeader ACCESS_CONTROL_ALLOW_HEADERS = register("Access-Control-Allow-Headers");
	public static final HttpHeader ACCESS_CONTROL_ALLOW_METHODS = register("Access-Control-Allow-Methods");
	public static final HttpHeader ACCESS_CONTROL_ALLOW_ORIGIN = register("Access-Control-Allow-Origin");
	public static final HttpHeader ACCESS_CONTROL_ALLOW_CREDENTIALS = register("Access-Control-Allow-Credentials");
	public static final HttpHeader ACCESS_CONTROL_EXPOSE_HEADERS = register("Access-Control-Expose-Headers");
	public static final HttpHeader ACCESS_CONTROL_MAX_AGE = register("Access-Control-Max-Age");
	public static final HttpHeader AGE = register("Age");
	public static final HttpHeader ALLOW = register("Allow");
	public static final HttpHeader CONTENT_DISPOSITION = register("Content-Disposition");
	public static final HttpHeader CONTENT_ENCODING = register("Content-Encoding");
	public static final HttpHeader CONTENT_LANGUAGE = register("Content-Language");
	public static final HttpHeader CONTENT_LOCATION = register("Content-Location");
	public static final HttpHeader CONTENT_MD5 = register("Content-MD5");
	public static final HttpHeader CONTENT_RANGE = register("Content-Range");
	public static final HttpHeader CONTENT_SECURITY_POLICY = register("Content-Security-Policy");
	public static final HttpHeader CONTENT_SECURITY_POLICY_REPORT_ONLY = register("Content-Security-Policy-Report-Only");
	public static final HttpHeader ETAG = register("ETag");
	public static final HttpHeader EXPIRES = register("Expires");
	public static final HttpHeader LAST_MODIFIED = register("Last-Modified");
	public static final HttpHeader LINK = register("Link");
	public static final HttpHeader LOCATION = register("Location");
	public static final HttpHeader P3P = register("P3P");
	public static final HttpHeader PROXY_AUTHENTICATE = register("Proxy-Authenticate");
	public static final HttpHeader REFRESH = register("Refresh");
	public static final HttpHeader RETRY_AFTER = register("Retry-After");
	public static final HttpHeader SERVER = register("Server");
	public static final HttpHeader SET_COOKIE = register("Set-Cookie");
	public static final HttpHeader SET_COOKIE2 = register("Set-Cookie2");
	public static final HttpHeader STRICT_TRANSPORT_SECURITY = register("Strict-Transport-Security");
	public static final HttpHeader TIMING_ALLOW_ORIGIN = register("Timing-Allow-Origin");
	public static final HttpHeader TRAILER = register("Trailer");
	public static final HttpHeader TRANSFER_ENCODING = register("Transfer-Encoding");
	public static final HttpHeader VARY = register("Vary");
	public static final HttpHeader WWW_AUTHENTICATE = register("WWW-Authenticate");
	public static final HttpHeader DNT = register("DNT");
	public static final HttpHeader X_CONTENT_TYPE_OPTIONS = register("X-Content-Type-Options");
	public static final HttpHeader X_DO_NOT_TRACK = register("X-Do-Not-Track");
	public static final HttpHeader X_FORWARDED_FOR = register("X-Forwarded-For");
	public static final HttpHeader X_FORWARDED_PROTO = register("X-Forwarded-Proto");
	public static final HttpHeader X_FRAME_OPTIONS = register("X-Frame-Options");
	public static final HttpHeader X_POWERED_BY = register("X-Powered-By");
	public static final HttpHeader PUBLIC_KEY_PINS = register("Public-Key-Pins");
	public static final HttpHeader PUBLIC_KEY_PINS_REPORT_ONLY = register("Public-Key-Pins-Report-Only");
	public static final HttpHeader X_REQUESTED_WITH = register("X-Requested-With");
	public static final HttpHeader X_USER_IP = register("X-User-IP");
	public static final HttpHeader X_XSS_PROTECTION = register("X-XSS-Protection");

	public static final HttpHeader X_REAL_IP = register("X-Real-IP");

	private static HttpHeader register(String name) {
		byte[] bytes = encodeAscii(name);

		byte[] lowerCaseBytes = new byte[bytes.length];
		int lowerCaseHashCode = 1;
		for (int i = 0; i < bytes.length; i++) {
			byte b = bytes[i];
			if (b >= 'A' && b <= 'Z')
				b += 'a' - 'A';
			lowerCaseBytes[i] = b;
			lowerCaseHashCode = lowerCaseHashCode * 31 + b;
		}
		HttpHeader httpHeader = new HttpHeader(bytes, 0, bytes.length, lowerCaseBytes, lowerCaseHashCode);

		assert Integer.bitCount(HEADERS.length) == 1;
		for (int p = 0; p < MAX_PROBINGS; p++) {
			int slot = (lowerCaseHashCode + p) & (HEADERS.length - 1);
			if (HEADERS[slot] == null) {
				HEADERS[slot] = httpHeader;
				return httpHeader;
			}
		}
		throw new IllegalArgumentException("HttpHeader hash collision, try to increase HEADERS size");
	}

	protected final byte[] bytes;
	protected final int offset;
	protected final int length;

	protected final byte[] lowerCaseBytes;
	protected final int lowerCaseHashCode;

	private HttpHeader(byte[] bytes, int offset, int length,
	                   byte[] lowerCaseBytes, int lowerCaseHashCode) {
		this.bytes = bytes;
		this.offset = offset;
		this.length = length;
		this.lowerCaseBytes = lowerCaseBytes;
		this.lowerCaseHashCode = lowerCaseHashCode;
	}

	static HttpHeader parseHeader(byte[] array, int offset, int length, int lowerCaseHashCode) {
		for (int p = 0; p < MAX_PROBINGS; p++) {
			int slot = (lowerCaseHashCode + p) & (HEADERS.length - 1);
			HttpHeader header = HEADERS[slot];
			if (header == null)
				break;
			if (header.lowerCaseHashCode == lowerCaseHashCode && equalsLowerCaseAscii(header.lowerCaseBytes, array, offset, length)) {
				return header;
			}
		}
		return new HttpCustomHeader(array, offset, length, lowerCaseHashCode);
	}

	public static HttpHeader headerOfString(String string) {
		byte[] array = encodeAscii(string);
		return parseHeader(array, 0, array.length, hashCodeLowerCaseAscii(array));
	}

	protected static final class HttpCustomHeader extends HttpHeader {
		private HttpCustomHeader(byte[] array, int offset, int length, int lowerCaseHashCode) {
			super(array, offset, length, null, lowerCaseHashCode);
		}

		@Override
		public boolean equals(Object o) {
			if (o == null || !(o instanceof HttpCustomHeader))
				return false;
			HttpCustomHeader that = (HttpCustomHeader) o;

			if (length != that.length) return false;
			for (int i = 0; i < length; i++) {
				byte thisChar = this.bytes[offset + i];
				byte thatChar = that.bytes[that.offset + i];
				if (thisChar >= 'A' && thisChar <= 'Z')
					thisChar += 'a' - 'A';
				if (thatChar >= 'A' && thatChar <= 'Z')
					thatChar += 'a' - 'A';
				if (thisChar != thatChar)
					return false;
			}
			return true;
		}
	}

	@Override
	public int hashCode() {
		return lowerCaseHashCode;
	}

	public int size() {
		return length;
	}

	@Override
	public String toString() {
		return decodeAscii(bytes, offset, length);
	}

	void writeTo(ByteBuf buf) {
		buf.put(bytes, offset, length);
	}

	static Value asBytes(HttpHeader key, byte[] array, int offset, int size) {
		return new ValueOfBytes(key, array, offset, size);
	}

	static Value asBytes(HttpHeader key, byte[] array) {
		return asBytes(key, array, 0, array.length);
	}

	static Value asBytes(HttpHeader key, String string) {
		return asBytes(key, encodeAscii(string));
	}

	static Value ofDecimal(HttpHeader key, int value) {
		return new ValueOfUnsignedDecimal(key, value);
	}

	static Value ofString(HttpHeader key, String string) {
		return new ValueOfString(key, string);
	}

	static Value ofCharsets(HttpHeader key, List<HttpUtils.Pair<Charset>> charsets) {
		return new ValueOfCharsets(key, charsets);
	}

	static Value ofCookies(HttpHeader key, List<HttpCookie> cookies) {
		return new ValueOfCookies(key, cookies);
	}

	static Value ofCookie(HttpHeader key, HttpCookie cookie) {
		return new ValueOfCookie(key, cookie);
	}

	static Value ofDate(HttpHeader key, Date date) {
		return new ValueOfDate(key, date);
	}

	static Value ofContentType(HttpHeader key, List<ContentType> type) {
		return new ValueOfContentTypes(key, type);
	}

	static abstract class Value {
		private final HttpHeader key;

		public Value(HttpHeader key) {
			this.key = key;
		}

		public abstract int estimateSize();

		public abstract void writeTo(ByteBuf buf);

		public HttpHeader getKey() {
			return key;
		}
	}

	private static final class ValueOfContentTypes extends Value {
		private final List<ContentType> types;

		public ValueOfContentTypes(HttpHeader key, List<ContentType> types) {
			super(key);
			this.types = types;
		}

		@Override
		public int estimateSize() {
			int size = 0;
			for (ContentType type : types) {
				size += type.estimateSize() + 2;
			}
			return size;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			ContentType.render(types, buf);
		}
	}

	private static final class ValueOfCookies extends Value {
		private final List<HttpCookie> cookies;

		public ValueOfCookies(HttpHeader key, List<HttpCookie> cookies) {
			super(key);
			this.cookies = cookies;
		}

		@Override
		public int estimateSize() {
			int size = 0;
			for (HttpCookie cookie : cookies) {
				size += cookie.getName().length();
				size += cookie.getValue() == null ? 0 : cookie.getValue().length();
			}
			return size;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			HttpCookie.render(cookies, buf);
		}
	}

	private static final class ValueOfCookie extends Value {
		private final HttpCookie cookie;

		public ValueOfCookie(HttpHeader key, HttpCookie cookie) {
			super(key);
			this.cookie = cookie;
		}

		@Override
		public int estimateSize() {
			int size = 0;
			size += cookie.getName().length();
			size += cookie.getValue() == null ? 0 : cookie.getValue().length();
			size += cookie.getDomain() == null ? 0 : cookie.getDomain().length() + 10;
			size += cookie.getPath() == null ? 0 : cookie.getPath().length() + 6;
			size += cookie.getExtension() == null ? 0 : cookie.getExtension().length();
			size += 100;
			return size;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			cookie.renderFull(buf);
		}
	}

	private static final class ValueOfCharsets extends Value {
		private final List<HttpUtils.Pair<Charset>> charsets;

		public ValueOfCharsets(HttpHeader key, List<HttpUtils.Pair<Charset>> charsets) {
			super(key);
			this.charsets = charsets;
		}

		@Override
		public int estimateSize() {
			return charsets.size() * 40;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			CharsetUtils.render(charsets, buf);
		}
	}

	private static final class ValueOfDate extends Value {
		private final Date date;

		public ValueOfDate(HttpHeader key, Date date) {
			super(key);
			this.date = date;
		}

		@Override
		public int estimateSize() {
			return 29;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			HttpDate.render(date.getTime(), buf);
		}
	}

	private static final class ValueOfBytes extends Value {
		private final byte[] array;
		private final int offset;
		private final int size;

		private ValueOfBytes(HttpHeader key, byte[] array, int offset, int size) {
			super(key);
			this.array = array;
			this.offset = offset;
			this.size = size;
		}

		@Override
		public int estimateSize() {
			return size;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			buf.put(array, offset, size);
		}

		@Override
		public String toString() {
			return decodeAscii(array, offset, size);
		}
	}

	private static final class ValueOfUnsignedDecimal extends Value {
		private final int value;

		private ValueOfUnsignedDecimal(HttpHeader header, int value) {
			super(header);
			this.value = value;
		}

		@Override
		public int estimateSize() {
			return 10; // Integer.toString(Integer.MAX_VALUE).length();
		}

		@Override
		public void writeTo(ByteBuf buf) {
			putDecimal(buf, value);
		}

		@Override
		public String toString() {
			return Integer.toString(value);
		}
	}

	private static final class ValueOfString extends Value {
		private final String string;

		private ValueOfString(HttpHeader key, String string) {
			super(key);
			this.string = string;
		}

		@Override
		public int estimateSize() {
			return string.length();
		}

		@Override
		public void writeTo(ByteBuf buf) {
			putAscii(buf, string);
		}

		@Override
		public String toString() {
			return string;
		}
	}
}
