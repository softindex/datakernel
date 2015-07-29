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

import java.net.HttpCookie;
import java.util.Collection;

import static io.datakernel.http.HttpHeader.*;
import static io.datakernel.util.ByteBufStrings.encodeAscii;
import static io.datakernel.util.ByteBufStrings.putDecimal;

/**
 * It represents the HTTP result with which server response in client {@link HttpRequest}. It must ave only
 * one owner in each  part of time. After handling in client this HTTP result it will be recycled and you can
 * not use it later.
 */
public final class HttpResponse extends HttpMessage {

	private final int code;

	private HttpResponse() {
		this(200);
	}

	private HttpResponse(int code) {
		this.code = code;
	}

	/**
	 * Creates a new HttpResponse with result code from argument
	 *
	 * @param code the code for new HttpResponse
	 * @return the new HttpResponse
	 */
	public static HttpResponse create(int code) {
		assert code >= 100 && code < 600;
		return new HttpResponse(code);
	}

	/**
	 * Returns HttpResponse with contains status 200
	 *
	 * @return new HttpResponse
	 */
	public static HttpResponse create() {
		return new HttpResponse();
	}

	/**
	 * Returns HttpResponse with contains status 302 and the URL for redirecting
	 *
	 * @param url the URL for redirecting
	 * @return new HttpResponse
	 */
	public static HttpResponse redirect302(String url) {
		return create(302)
				.header(HttpHeader.LOCATION, url);
	}

	// common builder methods

	/**
	 * Sets the header for this HttpResponse
	 *
	 * @param value value of header
	 * @return this HttpResponse
	 */
	public HttpResponse header(HttpHeaderValue value) {
		assert !recycled;
		setHeader(value);
		return this;
	}

	/**
	 * Sets the header with value as ByteBuf for this HttpResponse
	 *
	 * @param header header for this HttpResponse
	 * @param value  value o this header
	 * @return this HttpResponse
	 */
	public HttpResponse header(HttpHeader header, ByteBuf value) {
		assert !recycled;
		setHeader(header, value);
		return this;
	}

	/**
	 * Sets the header as array of bytes for this HttpResponse
	 *
	 * @param header header for this HttpResponse
	 * @param value  value of header
	 * @return this HttpResponse
	 */
	public HttpResponse header(HttpHeader header, byte[] value) {
		assert !recycled;
		setHeader(header, value);
		return this;
	}

	/**
	 * Sets the header as string for this HttpResponse
	 *
	 * @param header header for this HttpResponse
	 * @param value  value of header
	 * @return this HttpResponse
	 */
	public HttpResponse header(HttpHeader header, String value) {
		assert !recycled;
		setHeader(header, value);
		return this;
	}

	/**
	 * Sets the collection of headers to this HttpResponse
	 *
	 * @param headers collection with headers and its values
	 * @return this HttpResponse
	 */
	public HttpResponse headers(Collection<HttpHeaderValue> headers) {
		assert !recycled;
		setHeaders(headers);
		return this;
	}

	/**
	 * Sets the body to this HttpResponse as ByteBuf
	 *
	 * @param body new body
	 * @return this HttpResponse
	 */
	public HttpResponse body(ByteBuf body) {
		assert !recycled;
		setBody(body);
		return this;
	}

	/**
	 * Sets the body for this HttpResult as byte array
	 *
	 * @param array the new body
	 * @return this HttpResponse
	 */
	public HttpResponse body(byte[] array) {
		assert !recycled;
		return body(ByteBuf.wrap(array));
	}

	/**
	 * Sets header CONTENT_TYPE
	 *
	 * @param contentType value of header
	 * @return this HttpResponse
	 */
	public HttpResponse contentType(String contentType) {
		assert !recycled;
		setHeader(CONTENT_TYPE, contentType);
		return this;
	}

	// specific builder methods

	private static final HttpHeaderValue CACHE_CONTROL__NO_STORE = HttpHeader.asBytes(CACHE_CONTROL, "no-store");
	private static final HttpHeaderValue PRAGMA__NO_CACHE = HttpHeader.asBytes(PRAGMA, "no-cache");
	private static final HttpHeaderValue AGE__0 = HttpHeader.asBytes(AGE, "0");

	/**
	 * Sets the headers that means that it is no cache
	 *
	 * @return this HttpResponse
	 */
	public HttpResponse noCache() {
		assert !recycled;
		setHeader(CACHE_CONTROL__NO_STORE);
		setHeader(PRAGMA__NO_CACHE);
		setHeader(AGE__0);
		return this;
	}

	/**
	 * Adds the header SET_COOKIE
	 *
	 * @param cookie cookie for setting
	 * @return this HttpResponse
	 */
	public HttpResponse cookie(HttpCookie cookie) {
		assert !recycled;
		String s = HttpUtils.cookieToServerString(cookie);
		addHeader(HttpHeader.SET_COOKIE, s);
		return this;
	}

	public static HttpResponse notFound404() {
		return create(404);
	}

	public static HttpResponse internalServerError500() {
		return create(500);
	}

	/**
	 * Adds the header SET_COOKIE
	 *
	 * @param cookies collection with cookies for setting
	 * @return this HttpResponse
	 */
	public HttpResponse cookie(Collection<HttpCookie> cookies) {
		assert !recycled;
		for (HttpCookie cookie : cookies) {
			String s = HttpUtils.cookieToServerString(cookie);
			addHeader(HttpHeader.SET_COOKIE, s);
		}
		return this;
	}

	// getters

	public int getCode() {
		assert !recycled;
		return code;
	}

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
	public ByteBuf write() {
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
