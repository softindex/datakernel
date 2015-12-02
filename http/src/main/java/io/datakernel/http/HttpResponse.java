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

import com.google.common.collect.ImmutableMap;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.util.ByteBufStrings;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static io.datakernel.http.HttpHeader.*;
import static io.datakernel.util.ByteBufStrings.*;

/**
 * It represents the HTTP result with which server response in client {@link HttpRequest}. It must ave only
 * one owner in each  part of time. After handling in client this HTTP result it will be recycled and you can
 * not use it later.
 */
public final class HttpResponse extends HttpMessage {
	private final int code;

	private HttpResponse(int code) {
		this.code = code;
	}

	public static HttpResponse create(int code) {
		assert code >= 100 && code < 600;
		return new HttpResponse(code);
	}

	public static HttpResponse create() {
		return new HttpResponse(200);
	}

	public static HttpResponse redirect302(String url) {
		return create(302)
				.header(HttpHeader.LOCATION, url);
	}

	public static HttpResponse notFound404() {
		return create(404);
	}

	public static HttpResponse internalServerError500() {
		return create(500);
	}

	// common builder methods
	public HttpResponse header(HttpHeader header, ByteBuf value) {
		assert !recycled;
		setHeader(header, value);
		return this;
	}

	public HttpResponse header(HttpHeader header, byte[] value) {
		assert !recycled;
		setHeader(header, value);
		return this;
	}

	public HttpResponse header(HttpHeader header, String value) {
		assert !recycled;
		setHeader(header, value);
		return this;
	}

	public HttpResponse body(ByteBuf body) {
		assert !recycled;
		setBody(body);
		return this;
	}

	public HttpResponse body(byte[] array) {
		assert !recycled;
		return body(ByteBuf.wrap(array));
	}

	// specific builder methods
	private static final Value CACHE_CONTROL__NO_STORE = HttpHeader.asBytes(CACHE_CONTROL, "no-store");
	private static final Value PRAGMA__NO_CACHE = HttpHeader.asBytes(PRAGMA, "no-cache");
	private static final Value AGE__0 = HttpHeader.asBytes(AGE, "0");

	public HttpResponse noCache() {
		assert !recycled;
		setHeader(CACHE_CONTROL__NO_STORE);
		setHeader(PRAGMA__NO_CACHE);
		setHeader(AGE__0);
		return this;
	}

	public HttpResponse setAge(int value) {
		assert !recycled;
		setHeader(HttpHeader.AGE, value);
		return this;
	}

	public HttpResponse setContentType(ContentType contentType) {
		assert !recycled;
		addContentTypeHeader(CONTENT_TYPE, contentType);
		return this;
	}

	public HttpResponse setContentType(List<ContentType> contentTypes) {
		assert !recycled;
		addContentTypeHeader(CONTENT_TYPE, contentTypes);
		return this;
	}

	public HttpResponse setDate(Date value) {
		assert !recycled;
		setHeader(HttpHeader.DATE, value);
		return this;
	}

	public HttpResponse setExpires(Date value) {
		assert !recycled;
		setHeader(HttpHeader.EXPIRES, value);
		return this;
	}

	public HttpResponse setLastModified(Date value) {
		assert !recycled;
		setHeader(HttpHeader.LAST_MODIFIED, value);
		return this;
	}

	public HttpResponse setCookie(HttpCookie cookie) {
		assert !recycled;
		addCookieHeader(HttpHeader.SET_COOKIE, cookie);
		return this;
	}

	public HttpResponse setCookie(Collection<HttpCookie> cookies) {
		assert !recycled;
		for (HttpCookie cookie : cookies) {
			addCookieHeader(HttpHeader.SET_COOKIE, cookie);
		}
		return this;
	}

	// getters
	public int getCode() {
		assert !recycled;
		return code;
	}

	public int getAge() {
		assert !recycled;
		String value = getHeaderString(HttpHeader.AGE);
		if (value != null && !value.equals("")) {
			return decodeDecimal(value.getBytes(Charset.forName("ISO-8859-1")), 0, value.length());
		}
		return -1;
	}

	public Date getExpires() {
		assert !recycled;
		String value = getHeaderString(HttpHeader.EXPIRES);
		if (value != null && !value.equals("")) {
			long timestamp = HttpDate.parse(ByteBufStrings.encodeAscii(value), 0);
			return new Date(timestamp);
		}
		return null;
	}

	public Date getLastModified() {
		assert !recycled;
		String value = getHeaderString(HttpHeader.LAST_MODIFIED);
		if (value != null && !value.equals("")) {
			long timestamp = HttpDate.parse(ByteBufStrings.encodeAscii(value), 0);
			return new Date(timestamp);
		}
		return null;
	}

	public List<HttpCookie> getCookies() {
		assert !recycled;
		List<HttpCookie> cookie = new ArrayList<>();
		List<String> headers = getHeaderStrings(SET_COOKIE);
		for (String header : headers) {
			HttpCookie.parse(header, cookie);
		}
		return cookie;
	}

	// internal
	private static final byte[] HTTP11_BYTES = encodeAscii("HTTP/1.1 ");
	private static final byte[] CODE_ERROR_BYTES = encodeAscii(" Error");
	private static final byte[] CODE_OK_BYTES = encodeAscii(" OK");

	private static void writeCodeMessageEx(ByteBuf buf, int code) {
		buf.put(HTTP11_BYTES);
		putDecimal(buf, code);
		if (code >= 400) {
			buf.put(CODE_ERROR_BYTES);
		} else {
			buf.put(CODE_OK_BYTES);
		}
	}

	private static final byte[] CODE_200_BYTES = encodeAscii("HTTP/1.1 200 OK");
	private static final byte[] CODE_302_BYTES = encodeAscii("HTTP/1.1 302 Found");
	private static final byte[] CODE_400_BYTES = encodeAscii("HTTP/1.1 400 Bad Request");
	private static final byte[] CODE_403_BYTES = encodeAscii("HTTP/1.1 403 Forbidden");
	private static final byte[] CODE_404_BYTES = encodeAscii("HTTP/1.1 404 Not Found");
	private static final byte[] CODE_500_BYTES = encodeAscii("HTTP/1.1 500 Internal Server Error");
	private static final byte[] CODE_503_BYTES = encodeAscii("HTTP/1.1 503 Service Unavailable");
	private static final int LONGEST_FIRST_LINE_SIZE = CODE_503_BYTES.length;

	private static void writeCodeMessage(ByteBuf buf, int code) {
		byte[] result;
		switch (code) {
			case 200:
				result = CODE_200_BYTES;
				break;
			case 302:
				result = CODE_302_BYTES;
				break;
			case 400:
				result = CODE_400_BYTES;
				break;
			case 403:
				result = CODE_403_BYTES;
				break;
			case 404:
				result = CODE_404_BYTES;
				break;
			case 500:
				result = CODE_500_BYTES;
				break;
			case 503:
				result = CODE_503_BYTES;
				break;
			default:
				writeCodeMessageEx(buf, code);
				return;
		}
		buf.put(result);
	}

	private static final ImmutableMap<Integer, ByteBuf> DEFAULT_CODE_BODIES;

	static {
		DEFAULT_CODE_BODIES = ImmutableMap.<Integer, ByteBuf>builder()
				.put(400, ByteBuf.wrap(encodeAscii("Your browser (or proxy) sent a request that this server could not understand.")))
				.put(403, ByteBuf.wrap(encodeAscii("You don't have permission to access the requested directory.")))
				.put(404, ByteBuf.wrap(encodeAscii("The requested URL was not found on this server.")))
				.put(500, ByteBuf.wrap(encodeAscii("The server encountered an internal error and was unable to complete your request.")))
				.put(503, ByteBuf.wrap(encodeAscii("The server is temporarily unable to service your request due to maintenance downtime or capacity problems.")))
				.build();
	}

	/**
	 * Writes this HttpResult to pool-allocated ByteBuf with large enough size
	 *
	 * @return HttpResponse as ByteBuf
	 */
	ByteBuf write() {
		assert !recycled;
		if (code >= 400 && getBody() == null) {
			setBody(DEFAULT_CODE_BODIES.get(code));
		}
		setHeader(HttpHeader.ofDecimal(CONTENT_LENGTH, body == null ? 0 : body.remaining()));
		int estimateSize = estimateSize(LONGEST_FIRST_LINE_SIZE);
		ByteBuf buf = ByteBufPool.allocate(estimateSize);

		writeCodeMessage(buf, code);

		writeHeaders(buf);
		writeBody(buf);

		buf.flip();

		return buf;
	}

	@Override
	public String toString() {
		return HttpResponse.class.getSimpleName() + ": " + code;
	}
}