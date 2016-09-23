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
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.exception.ParseException;

import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.putDecimal;
import static io.datakernel.http.HttpHeaders.*;

/**
 * It represents the HTTP result with which server response in client {@link HttpRequest}. It must ave only
 * one owner in each  part of time. After handling in client this HTTP result it will be recycled and you can
 * not use it later.
 */
public final class HttpResponse extends HttpMessage {
	private static final Value CACHE_CONTROL__NO_STORE = HttpHeaders.asBytes(CACHE_CONTROL, "no-store");
	private static final Value PRAGMA__NO_CACHE = HttpHeaders.asBytes(PRAGMA, "no-cache");
	private static final Value AGE__0 = HttpHeaders.asBytes(AGE, "0");

	private final int code;

	// region builders
	private HttpResponse(int code) {
		this.code = code;
	}

	public static HttpResponse ofCode(int code) {
		assert code >= 100 && code < 600;
		return new HttpResponse(code);
	}

	public static HttpResponse ok200() {
		return new HttpResponse(200);
	}

	public static HttpResponse redirect302(String url) {
		HttpResponse response = HttpResponse.ofCode(302);
		response.addHeader(HttpHeaders.LOCATION, url);
		return response;
	}

	public static HttpResponse badRequest400() {
		return ofCode(400);
	}

	public static HttpResponse notFound404() {
		return ofCode(404);
	}

	public static HttpResponse internalServerError500() {
		return ofCode(500);
	}

	// common builder methods
	public HttpResponse withHeader(HttpHeader header, ByteBuf value) {
		addHeader(header, value);
		return this;
	}

	public HttpResponse withHeader(HttpHeader header, byte[] value) {
		addHeader(header, value);
		return this;
	}

	public HttpResponse withHeader(HttpHeader header, String value) {
		addHeader(header, value);
		return this;
	}

	public HttpResponse withBody(ByteBuf body) {
		setBody(body);
		return this;
	}

	public HttpResponse withBody(byte[] array) {
		setBody(array);
		return this;
	}

	// specific builder methods
	public HttpResponse withNoCache() {
		setNoCache();
		return this;
	}

	public HttpResponse withAge(int value) {
		setAge(value);
		return this;
	}

	public HttpResponse withContentType(ContentType contentType) {
		setContentType(contentType);
		return this;
	}

	public HttpResponse withDate(Date date) {
		setDate(date);
		return this;
	}

	public HttpResponse withExpires(Date date) {
		setExpires(date);
		return this;
	}

	public HttpResponse withLastModified(Date date) {
		setLastModified(date);
		return this;
	}

	public HttpResponse withCookies(List<HttpCookie> cookies) {
		setCookies(cookies);
		return this;
	}

	public HttpResponse withCookies(HttpCookie... cookies) {
		setCookies(cookies);
		return this;
	}

	public HttpResponse withCookie(HttpCookie cookie) {
		setCookie(cookie);
		return this;
	}
	// endregion

	// region setters
	public void setNoCache() {
		setHeader(CACHE_CONTROL__NO_STORE);
		setHeader(PRAGMA__NO_CACHE);
		setHeader(AGE__0);
	}

	public void setAge(int value) {
		setHeader(ofDecimal(AGE, value));
	}

	public void setContentType(ContentType contentType) {
		setHeader(ofContentType(HttpHeaders.CONTENT_TYPE, contentType));
	}

	public void setDate(Date date) {
		setHeader(ofDate(HttpHeaders.DATE, date));
	}

	public void setExpires(Date date) {
		setHeader(ofDate(HttpHeaders.EXPIRES, date));
	}

	public void setLastModified(Date date) {
		setHeader(ofDate(HttpHeaders.LAST_MODIFIED, date));
	}

	public void setCookies(List<HttpCookie> cookies) {
		addHeader(ofSetCookies(HttpHeaders.SET_COOKIE, cookies));
	}

	public void setCookies(HttpCookie... cookies) {
		setCookies(Arrays.asList(cookies));
	}

	public void setCookie(HttpCookie cookie) {
		setCookies(Collections.singletonList(cookie));
	}
	// endregion

	// region getters
	public int getCode() {
		assert !recycled;
		return code;
	}

	public int parseAge() throws ParseException {
		assert !recycled;
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeaderValue(AGE);
		if (header != null)
			return ByteBufStrings.decodeDecimal(header.array, header.offset, header.size);
		return 0;
	}

	public Date parseExpires() throws ParseException {
		assert !recycled;
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeaderValue(EXPIRES);
		if (header != null)
			return new Date(HttpDate.parse(header.array, header.offset));
		return null;
	}

	public Date parseLastModified() throws ParseException {
		assert !recycled;
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeaderValue(LAST_MODIFIED);
		if (header != null)
			return new Date(HttpDate.parse(header.array, header.offset));
		return null;
	}

	@Override
	public List<HttpCookie> parseCookies() throws ParseException {
		assert !recycled;
		List<HttpCookie> cookies = new ArrayList<>();
		List<Value> headers = getHeaderValues(SET_COOKIE);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			HttpCookie.parse(value.array, value.offset, value.offset + value.size, cookies);
		}
		return cookies;
	}
	// endregion

	// region internal
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

	private static final Map<Integer, byte[]> DEFAULT_CODE_BODIES;

	static {
		DEFAULT_CODE_BODIES = new HashMap<>();
		DEFAULT_CODE_BODIES.put(400, encodeAscii("Your browser (or proxy) sent a request that this server could not understand."));
		DEFAULT_CODE_BODIES.put(403, encodeAscii("You don't have permission to access the requested directory."));
		DEFAULT_CODE_BODIES.put(404, encodeAscii("The requested URL was not found on this server."));
		DEFAULT_CODE_BODIES.put(500, encodeAscii("The server encountered an internal error and was unable to complete your request."));
		DEFAULT_CODE_BODIES.put(503, encodeAscii("The server is temporarily unable to service your request due to maintenance downtime or capacity problems."));
	}

	/**
	 * Writes this HttpResult to pool-allocated ByteBuf with large enough size
	 *
	 * @return HttpResponse as ByteBuf
	 */
	ByteBuf write() {
		assert !recycled;
		if (code >= 400 && getBody() == null) {
			byte[] bytes = DEFAULT_CODE_BODIES.get(code);
			setBody(bytes != null ? ByteBuf.wrapForReading(bytes) : null);
		}
		setHeader(HttpHeaders.ofDecimal(CONTENT_LENGTH, body == null ? 0 : body.headRemaining()));
		int estimateSize = estimateSize(LONGEST_FIRST_LINE_SIZE);
		ByteBuf buf = ByteBufPool.allocate(estimateSize);

		writeCodeMessage(buf, code);

		writeHeaders(buf);
		writeBody(buf);

		return buf;
	}

	@Override
	public String toString() {
		return HttpResponse.class.getSimpleName() + ": " + code;
	}
	// endregion
}