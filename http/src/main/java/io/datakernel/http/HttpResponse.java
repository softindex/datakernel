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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.util.ByteBufStrings;

import java.util.*;

import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.util.ByteBufStrings.encodeAscii;
import static io.datakernel.util.ByteBufStrings.putDecimal;

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
				.header(HttpHeaders.LOCATION, url);
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

	public HttpResponse body(byte[] array) {
		assert !recycled;
		return body(ByteBuf.wrap(array));
	}

	public HttpResponse body(ByteBuf body) {
		assert !recycled;
		setBody(body);
		return this;
	}

	// specific builder methods
	private static final Value CACHE_CONTROL__NO_STORE = HttpHeaders.asBytes(CACHE_CONTROL, "no-store");
	private static final Value PRAGMA__NO_CACHE = HttpHeaders.asBytes(PRAGMA, "no-cache");
	private static final Value AGE__0 = HttpHeaders.asBytes(AGE, "0");

	public HttpResponse noCache() {
		assert !recycled;
		setHeader(CACHE_CONTROL__NO_STORE);
		setHeader(PRAGMA__NO_CACHE);
		setHeader(AGE__0);
		return this;
	}

	public HttpResponse age(int value) {
		assert !recycled;
		setHeader(ofDecimal(AGE, value));
		return this;
	}

	public HttpResponse contentType(ContentType value) {
		assert !recycled;
		setHeader(ofContentType(HttpHeaders.CONTENT_TYPE, value));
		return this;
	}

	public HttpResponse date(Date value) {
		assert !recycled;
		setHeader(ofDate(HttpHeaders.DATE, value));
		return this;
	}

	public HttpResponse expires(Date value) {
		assert !recycled;
		setHeader(ofDate(HttpHeaders.EXPIRES, value));
		return this;
	}

	public HttpResponse lastModified(Date value) {
		assert !recycled;
		setHeader(ofDate(HttpHeaders.LAST_MODIFIED, value));
		return this;
	}

	public HttpResponse serverCookie(List<HttpCookie> values) {
		assert !recycled;
		addHeader(ofSetCookies(HttpHeaders.SET_COOKIE, values));
		return this;
	}

	// getters
	public int getCode() {
		assert !recycled;
		return code;
	}

	public int getAge() {
		assert !recycled;
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeader(AGE);
		if (header != null)
			return ByteBufStrings.decodeDecimal(header.array, header.offset, header.size);
		return 0;
	}

	public Date getExpires() {
		assert !recycled;
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeader(EXPIRES);
		if (header != null)
			return new Date(HttpDate.parse(header.array, header.offset));
		return null;
	}

	public Date getLastModified() {
		assert !recycled;
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeader(LAST_MODIFIED);
		if (header != null)
			return new Date(HttpDate.parse(header.array, header.offset));
		return null;
	}

	public List<HttpCookie> getCookies() {
		assert !recycled;
		List<HttpCookie> cookie = new ArrayList<>();
		List<Value> headers = getHeaders(SET_COOKIE);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			HttpCookie.parse(value.array, value.offset, value.offset + value.size, cookie);
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

	private static final Map<Integer, ByteBuf> DEFAULT_CODE_BODIES;

	static {
		DEFAULT_CODE_BODIES = new HashMap<>();
		DEFAULT_CODE_BODIES.put(400, ByteBuf.wrap(encodeAscii("Your browser (or proxy) sent a request that this server could not understand.")));
		DEFAULT_CODE_BODIES.put(403, ByteBuf.wrap(encodeAscii("You don't have permission to access the requested directory.")));
		DEFAULT_CODE_BODIES.put(404, ByteBuf.wrap(encodeAscii("The requested URL was not found on this server.")));
		DEFAULT_CODE_BODIES.put(500, ByteBuf.wrap(encodeAscii("The server encountered an internal error and was unable to complete your request.")));
		DEFAULT_CODE_BODIES.put(503, ByteBuf.wrap(encodeAscii("The server is temporarily unable to service your request due to maintenance downtime or capacity problems.")));
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
		setHeader(HttpHeaders.ofDecimal(CONTENT_LENGTH, body == null ? 0 : body.remaining()));
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