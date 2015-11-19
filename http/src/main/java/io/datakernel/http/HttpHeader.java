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

import com.google.common.base.Charsets;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.datakernel.bytebuf.ByteBuf;

import java.net.HttpCookie;
import java.nio.charset.Charset;
import java.util.List;

import static io.datakernel.util.ByteBufStrings.*;

/**
 * Represent the header {@link HttpResponse}
 */
public class HttpHeader {
	private static final int HEADERS_SLOTS = 512;
	private static final int MAX_PROBINGS = 2;

	private static final HttpHeader[] HEADERS = new HttpHeader[HEADERS_SLOTS];

	public static final HttpHeader CACHE_CONTROL = register(HttpHeaders.CACHE_CONTROL);
	public static final HttpHeader CONTENT_LENGTH = register(HttpHeaders.CONTENT_LENGTH);
	public static final HttpHeader CONTENT_TYPE = register(HttpHeaders.CONTENT_TYPE);
	public static final HttpHeader DATE = register(HttpHeaders.DATE);
	public static final HttpHeader PRAGMA = register(HttpHeaders.PRAGMA);
	public static final HttpHeader VIA = register(HttpHeaders.VIA);
	public static final HttpHeader WARNING = register(HttpHeaders.WARNING);
	public static final HttpHeader ACCEPT = register(HttpHeaders.ACCEPT);
	public static final HttpHeader ACCEPT_CHARSET = register(HttpHeaders.ACCEPT_CHARSET);
	public static final HttpHeader ACCEPT_ENCODING = register(HttpHeaders.ACCEPT_ENCODING);
	public static final HttpHeader ACCEPT_LANGUAGE = register(HttpHeaders.ACCEPT_LANGUAGE);
	public static final HttpHeader ACCESS_CONTROL_REQUEST_HEADERS = register(HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS);
	public static final HttpHeader ACCESS_CONTROL_REQUEST_METHOD = register(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD);
	public static final HttpHeader AUTHORIZATION = register(HttpHeaders.AUTHORIZATION);
	public static final HttpHeader CONNECTION = register(HttpHeaders.CONNECTION);
	public static final HttpHeader COOKIE = register(HttpHeaders.COOKIE);
	public static final HttpHeader EXPECT = register(HttpHeaders.EXPECT);
	public static final HttpHeader FROM = register(HttpHeaders.FROM);
	public static final HttpHeader FOLLOW_ONLY_WHEN_PRERENDER_SHOWN = register(HttpHeaders.FOLLOW_ONLY_WHEN_PRERENDER_SHOWN);
	public static final HttpHeader HOST = register(HttpHeaders.HOST);
	public static final HttpHeader IF_MATCH = register(HttpHeaders.IF_MATCH);
	public static final HttpHeader IF_MODIFIED_SINCE = register(HttpHeaders.IF_MODIFIED_SINCE);
	public static final HttpHeader IF_NONE_MATCH = register(HttpHeaders.IF_NONE_MATCH);
	public static final HttpHeader IF_RANGE = register(HttpHeaders.IF_RANGE);
	public static final HttpHeader IF_UNMODIFIED_SINCE = register(HttpHeaders.IF_UNMODIFIED_SINCE);
	public static final HttpHeader LAST_EVENT_ID = register(HttpHeaders.LAST_EVENT_ID);
	public static final HttpHeader MAX_FORWARDS = register(HttpHeaders.MAX_FORWARDS);
	public static final HttpHeader ORIGIN = register(HttpHeaders.ORIGIN);
	public static final HttpHeader PROXY_AUTHORIZATION = register(HttpHeaders.PROXY_AUTHORIZATION);
	public static final HttpHeader RANGE = register(HttpHeaders.RANGE);
	public static final HttpHeader REFERER = register(HttpHeaders.REFERER);
	public static final HttpHeader TE = register(HttpHeaders.TE);
	public static final HttpHeader UPGRADE = register(HttpHeaders.UPGRADE);
	public static final HttpHeader USER_AGENT = register(HttpHeaders.USER_AGENT);
	public static final HttpHeader ACCEPT_RANGES = register(HttpHeaders.ACCEPT_RANGES);
	public static final HttpHeader ACCESS_CONTROL_ALLOW_HEADERS = register(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS);
	public static final HttpHeader ACCESS_CONTROL_ALLOW_METHODS = register(HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS);
	public static final HttpHeader ACCESS_CONTROL_ALLOW_ORIGIN = register(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN);
	public static final HttpHeader ACCESS_CONTROL_ALLOW_CREDENTIALS = register(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS);
	public static final HttpHeader ACCESS_CONTROL_EXPOSE_HEADERS = register(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS);
	public static final HttpHeader ACCESS_CONTROL_MAX_AGE = register(HttpHeaders.ACCESS_CONTROL_MAX_AGE);
	public static final HttpHeader AGE = register(HttpHeaders.AGE);
	public static final HttpHeader ALLOW = register(HttpHeaders.ALLOW);
	public static final HttpHeader CONTENT_DISPOSITION = register(HttpHeaders.CONTENT_DISPOSITION);
	public static final HttpHeader CONTENT_ENCODING = register(HttpHeaders.CONTENT_ENCODING);
	public static final HttpHeader CONTENT_LANGUAGE = register(HttpHeaders.CONTENT_LANGUAGE);
	public static final HttpHeader CONTENT_LOCATION = register(HttpHeaders.CONTENT_LOCATION);
	public static final HttpHeader CONTENT_MD5 = register(HttpHeaders.CONTENT_MD5);
	public static final HttpHeader CONTENT_RANGE = register(HttpHeaders.CONTENT_RANGE);
	public static final HttpHeader CONTENT_SECURITY_POLICY = register(HttpHeaders.CONTENT_SECURITY_POLICY);
	public static final HttpHeader CONTENT_SECURITY_POLICY_REPORT_ONLY = register(HttpHeaders.CONTENT_SECURITY_POLICY_REPORT_ONLY);
	public static final HttpHeader ETAG = register(HttpHeaders.ETAG);
	public static final HttpHeader EXPIRES = register(HttpHeaders.EXPIRES);
	public static final HttpHeader LAST_MODIFIED = register(HttpHeaders.LAST_MODIFIED);
	public static final HttpHeader LINK = register(HttpHeaders.LINK);
	public static final HttpHeader LOCATION = register(HttpHeaders.LOCATION);
	public static final HttpHeader P3P = register(HttpHeaders.P3P);
	public static final HttpHeader PROXY_AUTHENTICATE = register(HttpHeaders.PROXY_AUTHENTICATE);
	public static final HttpHeader REFRESH = register(HttpHeaders.REFRESH);
	public static final HttpHeader RETRY_AFTER = register(HttpHeaders.RETRY_AFTER);
	public static final HttpHeader SERVER = register(HttpHeaders.SERVER);
	public static final HttpHeader SET_COOKIE = register(HttpHeaders.SET_COOKIE);
	public static final HttpHeader SET_COOKIE2 = register(HttpHeaders.SET_COOKIE2);
	public static final HttpHeader STRICT_TRANSPORT_SECURITY = register(HttpHeaders.STRICT_TRANSPORT_SECURITY);
	public static final HttpHeader TIMING_ALLOW_ORIGIN = register(HttpHeaders.TIMING_ALLOW_ORIGIN);
	public static final HttpHeader TRAILER = register(HttpHeaders.TRAILER);
	public static final HttpHeader TRANSFER_ENCODING = register(HttpHeaders.TRANSFER_ENCODING);
	public static final HttpHeader VARY = register(HttpHeaders.VARY);
	public static final HttpHeader WWW_AUTHENTICATE = register(HttpHeaders.WWW_AUTHENTICATE);
	public static final HttpHeader DNT = register(HttpHeaders.DNT);
	public static final HttpHeader X_CONTENT_TYPE_OPTIONS = register(HttpHeaders.X_CONTENT_TYPE_OPTIONS);
	public static final HttpHeader X_DO_NOT_TRACK = register(HttpHeaders.X_DO_NOT_TRACK);
	public static final HttpHeader X_FORWARDED_FOR = register(HttpHeaders.X_FORWARDED_FOR);
	public static final HttpHeader X_FORWARDED_PROTO = register(HttpHeaders.X_FORWARDED_PROTO);
	public static final HttpHeader X_FRAME_OPTIONS = register(HttpHeaders.X_FRAME_OPTIONS);
	public static final HttpHeader X_POWERED_BY = register(HttpHeaders.X_POWERED_BY);
	public static final HttpHeader PUBLIC_KEY_PINS = register(HttpHeaders.PUBLIC_KEY_PINS);
	public static final HttpHeader PUBLIC_KEY_PINS_REPORT_ONLY = register(HttpHeaders.PUBLIC_KEY_PINS_REPORT_ONLY);
	public static final HttpHeader X_REQUESTED_WITH = register(HttpHeaders.X_REQUESTED_WITH);
	public static final HttpHeader X_USER_IP = register(HttpHeaders.X_USER_IP);
	public static final HttpHeader X_XSS_PROTECTION = register(HttpHeaders.X_XSS_PROTECTION);

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

	public void writeTo(ByteBuf buf) {
		buf.put(bytes, offset, length);
	}

	public static HttpHeaderValue asBytes(HttpHeader key, byte[] array, int offset, int size) {
		return new HttpHeaderValueOfBytes(key, array, offset, size);
	}

	public static HttpHeaderValue asBytes(HttpHeader key, byte[] array) {
		return asBytes(key, array, 0, array.length);
	}

	public static HttpHeaderValue asBytes(HttpHeader key, String string) {
		return asBytes(key, encodeAscii(string));
	}

	public static HttpHeaderValue ofList(HttpHeader key, List<HttpHeaderValue> values, char separator) {
		return new HttpHeaderValueOfList(key, values, separator);
	}

	public static HttpHeaderValue ofDecimal(HttpHeader key, int value) {
		return new HttpHeaderValueOfUnsignedDecimal(key, value);
	}

	public static HttpHeaderValue ofString(HttpHeader key, String string) {
		return new HttpHeaderValueOfString(key, string);
	}

	public static HttpHeaderValue ofCharset(HttpHeader key, Charset charset) {
		return new HttpHeaderValueOfCharset(key, charset);
	}

	public static HttpHeaderValue ofCookies(HttpHeader key, List<HttpCookie> cookies) {
		return new HttpHeaderValueOfCookies(key, cookies);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	private static final class HttpHeaderValueofContentType extends HttpHeaderValue {
		private final MediaType type;

		public HttpHeaderValueofContentType(HttpHeader key, MediaType type) {
			super(key);
			this.type = type;
		}

		@Override
		public int estimateSize() {
			return 0;
		}

		@Override
		public void writeTo(ByteBuf buf) {

		}
	}

	private static final class HttpHeaderValueOfCookies extends HttpHeaderValue {
		// FIXME using java.net cookies - impossible to operate with some attributes
		private final List<HttpCookie> cookies;

		public HttpHeaderValueOfCookies(HttpHeader key, List<HttpCookie> cookies) {
			super(key);
			this.cookies = cookies;
		}

		@Override
		public int estimateSize() {
			int size = 0;
			for (HttpCookie cookie : cookies) {
				size += cookie.getName().length();
				size += cookie.getValue().length();
				size += 50; // expires-av
				size += 20; // max age-av
				size += cookie.getDomain() == null ? 0 : cookie.getDomain().length() + 10; // domain-av
				size += cookie.getPath() == null ? 0 : cookie.getPath().length() + 6; // path-av
				size += 14; // Secure + HttpOnly
			}
			return size;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			// rfc 6265 due to java.net.HttpCookie restrictions impossible to implement all the fields
			for (HttpCookie cookie : cookies) {
				// name-value pair
				putAscii(buf, cookie.getName());
				putAscii(buf, "=\"");
				putAscii(buf, cookie.getValue());
				putAscii(buf, "\"; ");

				putAttr(buf, "Expires=", null);
				putAttr(buf, "Max-Age=", (int) cookie.getMaxAge());
				putAttr(buf, "Domain=", cookie.getDomain());
				putAttr(buf, "Path=", cookie.getPath());
				if (cookie.getSecure()) {
					putAscii(buf, "Secure; ");
				}
				if (cookie.isHttpOnly()) {
					putAscii(buf, "HttpOnly; ");
				}
				// extensions
			}
		}

		private void putAttr(ByteBuf buf, String name, String attr) {
			if (attr != null) {
				putAscii(buf, name);
				putAscii(buf, attr);
				putAscii(buf, "; ");
			}
		}

		private void putAttr(ByteBuf buf, String name, int attr) {
			if (attr != 0) {
				putAscii(buf, name);
				putDecimal(buf, attr);
				putAscii(buf, "; ");
			}
		}
	}

	private static final class HttpHeaderValueOfCharset extends HttpHeaderValue {
		private final Charset charset;

		public HttpHeaderValueOfCharset(HttpHeader key, Charset charset) {
			super(key);
			this.charset = charset;
		}

		@Override
		public int estimateSize() {
			return charset.name().length();
		}

		@Override
		public void writeTo(ByteBuf buf) {
			buf.put(charset.name().getBytes(Charsets.UTF_8));
		}
	}

	private static final class HttpHeaderValueOfBytes extends HttpHeaderValue {
		private final byte[] array;
		private final int offset;
		private final int size;

		private HttpHeaderValueOfBytes(HttpHeader key, byte[] array, int offset, int size) {
			super(key);
			this.array = array;
			this.offset = offset;
			this.size = size;
		}

		public byte[] array() {
			return array;
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

	private static final class HttpHeaderValueOfList extends HttpHeaderValue {
		private final List<HttpHeaderValue> values;
		private final byte separator;

		private HttpHeaderValueOfList(HttpHeader key, List<HttpHeaderValue> values, char separator) {
			super(key);
			this.values = values;
			this.separator = (byte) separator;
		}

		@Override
		public int estimateSize() {
			int result = values.size();
			for (HttpHeaderValue value : values) {
				result += value.estimateSize();
			}
			return result;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			boolean first = true;
			for (HttpHeaderValue value : values) {
				if (!first) {
					buf.put(separator);
				}
				first = false;
				value.writeTo(buf);
			}
		}

		@Override
		public String toString() {
			return null; // TODO decodeString(bytes, offset, size);
		}
	}

	private static final class HttpHeaderValueOfUnsignedDecimal extends HttpHeaderValue {
		private final int value;

		private HttpHeaderValueOfUnsignedDecimal(HttpHeader header, int value) {
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

	private static final class HttpHeaderValueOfString extends HttpHeaderValue {
		private final String string;

		private HttpHeaderValueOfString(HttpHeader key, String string) {
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
