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

import java.util.Date;
import java.util.List;

import static io.datakernel.util.ByteBufStrings.*;

@SuppressWarnings("unused")
public final class HttpHeaders {
	private static final CaseInsensitiveTokenMap<HttpHeader> headers = new CaseInsensitiveTokenMap<HttpHeader>(512, 2, HttpHeader.class) {
		@Override
		protected HttpHeader create(byte[] bytes, int offset, int length, byte[] lowerCaseBytes, int lowerCaseHashCode) {
			return new HttpHeader(bytes, offset, length, lowerCaseBytes, lowerCaseHashCode);
		}
	};

	public static final HttpHeader CACHE_CONTROL = headers.register("Cache-Control");
	public static final HttpHeader CONTENT_LENGTH = headers.register("Content-Length");
	public static final HttpHeader CONTENT_TYPE = headers.register("Content-Type");
	public static final HttpHeader DATE = headers.register("Date");
	public static final HttpHeader PRAGMA = headers.register("Pragma");
	public static final HttpHeader VIA = headers.register("Via");
	public static final HttpHeader WARNING = headers.register("Warning");
	public static final HttpHeader ACCEPT = headers.register("Accept");
	public static final HttpHeader ACCEPT_CHARSET = headers.register("Accept-Charset");
	public static final HttpHeader ACCEPT_ENCODING = headers.register("Accept-Encoding");
	public static final HttpHeader ACCEPT_LANGUAGE = headers.register("Accept-Language");
	public static final HttpHeader ACCESS_CONTROL_REQUEST_HEADERS = headers.register("Access-Control-Request-Headers");
	public static final HttpHeader ACCESS_CONTROL_REQUEST_METHOD = headers.register("Access-Control-Request-Method");
	public static final HttpHeader AUTHORIZATION = headers.register("Authorization");
	public static final HttpHeader CONNECTION = headers.register("Connection");
	public static final HttpHeader COOKIE = headers.register("Cookie");
	public static final HttpHeader EXPECT = headers.register("Expect");
	public static final HttpHeader FROM = headers.register("From");
	public static final HttpHeader FOLLOW_ONLY_WHEN_PRERENDER_SHOWN = headers.register("Follow-Only-When-Prerender-Shown");
	public static final HttpHeader HOST = headers.register("Host");
	public static final HttpHeader IF_MATCH = headers.register("If-Match");
	public static final HttpHeader IF_MODIFIED_SINCE = headers.register("If-Modified-Since");
	public static final HttpHeader IF_NONE_MATCH = headers.register("If-None-Match");
	public static final HttpHeader IF_RANGE = headers.register("If-Range");
	public static final HttpHeader IF_UNMODIFIED_SINCE = headers.register("If-Unmodified-Since");
	public static final HttpHeader LAST_EVENT_ID = headers.register("Last-Event-ID");
	public static final HttpHeader MAX_FORWARDS = headers.register("Max-Forwards");
	public static final HttpHeader ORIGIN = headers.register("Origin");
	public static final HttpHeader PROXY_AUTHORIZATION = headers.register("Proxy-Authorization");
	public static final HttpHeader RANGE = headers.register("Range");
	public static final HttpHeader REFERER = headers.register("Referer");
	public static final HttpHeader TE = headers.register("TE");
	public static final HttpHeader UPGRADE = headers.register("Upgrade");
	public static final HttpHeader USER_AGENT = headers.register("User-Agent");
	public static final HttpHeader ACCEPT_RANGES = headers.register("Accept-Ranges");
	public static final HttpHeader ACCESS_CONTROL_ALLOW_HEADERS = headers.register("Access-Control-Allow-Headers");
	public static final HttpHeader ACCESS_CONTROL_ALLOW_METHODS = headers.register("Access-Control-Allow-Methods");
	public static final HttpHeader ACCESS_CONTROL_ALLOW_ORIGIN = headers.register("Access-Control-Allow-Origin");
	public static final HttpHeader ACCESS_CONTROL_ALLOW_CREDENTIALS = headers.register("Access-Control-Allow-Credentials");
	public static final HttpHeader ACCESS_CONTROL_EXPOSE_HEADERS = headers.register("Access-Control-Expose-Headers");
	public static final HttpHeader ACCESS_CONTROL_MAX_AGE = headers.register("Access-Control-Max-Age");
	public static final HttpHeader AGE = headers.register("Age");
	public static final HttpHeader ALLOW = headers.register("Allow");
	public static final HttpHeader CONTENT_DISPOSITION = headers.register("Content-Disposition");
	public static final HttpHeader CONTENT_ENCODING = headers.register("Content-Encoding");
	public static final HttpHeader CONTENT_LANGUAGE = headers.register("Content-Language");
	public static final HttpHeader CONTENT_LOCATION = headers.register("Content-Location");
	public static final HttpHeader CONTENT_MD5 = headers.register("Content-MD5");
	public static final HttpHeader CONTENT_RANGE = headers.register("Content-Range");
	public static final HttpHeader CONTENT_SECURITY_POLICY = headers.register("Content-Security-Policy");
	public static final HttpHeader CONTENT_SECURITY_POLICY_REPORT_ONLY = headers.register("Content-Security-Policy-Report-Only");
	public static final HttpHeader ETAG = headers.register("ETag");
	public static final HttpHeader EXPIRES = headers.register("Expires");
	public static final HttpHeader LAST_MODIFIED = headers.register("Last-Modified");
	public static final HttpHeader LINK = headers.register("Link");
	public static final HttpHeader LOCATION = headers.register("Location");
	public static final HttpHeader P3P = headers.register("P3P");
	public static final HttpHeader PROXY_AUTHENTICATE = headers.register("Proxy-Authenticate");
	public static final HttpHeader REFRESH = headers.register("Refresh");
	public static final HttpHeader RETRY_AFTER = headers.register("Retry-After");
	public static final HttpHeader SERVER = headers.register("Server");
	public static final HttpHeader SET_COOKIE = headers.register("Set-Cookie");
	@Deprecated
	public static final HttpHeader SET_COOKIE2 = headers.register("Set-Cookie2");
	public static final HttpHeader STRICT_TRANSPORT_SECURITY = headers.register("Strict-Transport-Security");
	public static final HttpHeader TIMING_ALLOW_ORIGIN = headers.register("Timing-Allow-Origin");
	public static final HttpHeader TRAILER = headers.register("Trailer");
	public static final HttpHeader TRANSFER_ENCODING = headers.register("Transfer-Encoding");
	public static final HttpHeader VARY = headers.register("Vary");
	public static final HttpHeader WWW_AUTHENTICATE = headers.register("WWW-Authenticate");
	public static final HttpHeader DNT = headers.register("DNT");
	public static final HttpHeader X_CONTENT_TYPE_OPTIONS = headers.register("X-Content-Type-Options");
	public static final HttpHeader X_DO_NOT_TRACK = headers.register("X-Do-Not-Track");
	public static final HttpHeader X_FORWARDED_FOR = headers.register("X-Forwarded-For");
	public static final HttpHeader X_FORWARDED_PROTO = headers.register("X-Forwarded-Proto");
	public static final HttpHeader X_FRAME_OPTIONS = headers.register("X-Frame-Options");
	public static final HttpHeader X_POWERED_BY = headers.register("X-Powered-By");
	public static final HttpHeader PUBLIC_KEY_PINS = headers.register("Public-Key-Pins");
	public static final HttpHeader PUBLIC_KEY_PINS_REPORT_ONLY = headers.register("Public-Key-Pins-Report-Only");
	public static final HttpHeader X_REQUESTED_WITH = headers.register("X-Requested-With");
	public static final HttpHeader X_USER_IP = headers.register("X-User-IP");
	public static final HttpHeader X_XSS_PROTECTION = headers.register("X-XSS-Protection");

	public static final HttpHeader X_REAL_IP = headers.register("X-Real-IP");

	static HttpHeader parseHeader(byte[] array, int offset, int length, int lowerCaseHashCode) {
		return headers.get(array, offset, length, lowerCaseHashCode);
	}

	static HttpHeader parse(String string) {
		byte[] array = encodeAscii(string);
		int hash = hashCodeLowerCaseAscii(array);
		HttpHeader header = headers.get(array, 0, array.length, hash);
		return header != null ? header : new HttpHeader(array, 0, array.length, null, hash);
	}

	// helpers
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

	static Value ofCharsets(HttpHeader key, List<AcceptCharset> charsets) {
		return new ValueOfCharsets(key, charsets);
	}

	static Value ofCookies(HttpHeader key, List<HttpCookie> cookies) {
		return new ValueOfSimpleCookies(key, cookies);
	}

	static Value ofSetCookies(HttpHeader key, List<HttpCookie> cookies) {
		return new ValueOfFullCookies(key, cookies);
	}

	static Value ofDate(HttpHeader key, Date date) {
		return new ValueOfDate(key, date);
	}

	static Value ofAcceptContentTypes(HttpHeader key, List<AcceptMediaType> type) {
		return new ValueOfAcceptContentTypes(key, type);
	}

	static Value ofContentType(HttpHeader key, ContentType type) {
		return new ValueOfContentType(key, type);
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

	static final class ValueOfBytes extends Value {
		final byte[] array;
		final int offset;
		final int size;

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

	private static final class ValueOfContentType extends Value {
		private ContentType type;

		public ValueOfContentType(HttpHeader key, ContentType type) {
			super(key);
			this.type = type;
		}

		@Override
		public int estimateSize() {
			return type.size();
		}

		@Override
		public void writeTo(ByteBuf buf) {
			ContentType.render(type, buf);
		}

		@Override
		public String toString() {
			return type.toString();
		}
	}

	private static final class ValueOfAcceptContentTypes extends Value {
		private final List<AcceptMediaType> types;

		public ValueOfAcceptContentTypes(HttpHeader key, List<AcceptMediaType> types) {
			super(key);
			this.types = types;
		}

		@Override
		public int estimateSize() {
			int size = 0;
			for (AcceptMediaType type : types) {
				size += type.estimateSize() + 2;
			}
			return size;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			AcceptMediaType.render(types, buf);
		}
	}

	private static final class ValueOfSimpleCookies extends Value {
		private final List<HttpCookie> cookies;

		public ValueOfSimpleCookies(HttpHeader key, List<HttpCookie> cookies) {
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
			HttpCookie.renderSimple(cookies, buf);
		}
	}

	private static final class ValueOfFullCookies extends Value {
		private final List<HttpCookie> cookies;

		public ValueOfFullCookies(HttpHeader key, List<HttpCookie> cookies) {
			super(key);
			this.cookies = cookies;
		}

		@Override
		public int estimateSize() {
			int size = 0;
			for (HttpCookie cookie : cookies) {
				size += cookie.getName().length();
				size += cookie.getValue() == null ? 0 : cookie.getValue().length();
				size += cookie.getDomain() == null ? 0 : cookie.getDomain().length() + 10;
				size += cookie.getPath() == null ? 0 : cookie.getPath().length() + 6;
				size += cookie.getExtension() == null ? 0 : cookie.getExtension().length();
				size += 102;
			}
			return size;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			HttpCookie.renderFull(cookies, buf);
		}
	}

	private static final class ValueOfCharsets extends Value {
		private final List<AcceptCharset> charsets;

		public ValueOfCharsets(HttpHeader key, List<AcceptCharset> charsets) {
			super(key);
			this.charsets = charsets;
		}

		@Override
		public int estimateSize() {
			int size = 0;
			for (AcceptCharset charset : charsets) {
				size += charset.estimateSize() + 2;
			}
			return size;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			AcceptCharset.render(charsets, buf);
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
			long timestamp = date.getTime();
			HttpDate.render(date.getTime(), buf);
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
