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

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.exception.ParseException;
import io.datakernel.util.Initializable;

import java.nio.charset.Charset;
import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.putDecimal;
import static io.datakernel.http.HttpHeaders.*;

/**
 * Represents HTTP response for {@link HttpRequest}. After handling {@code HttpResponse} will be recycled so you cannot
 * usi it afterwards.
 */
public final class HttpResponse extends HttpMessage implements Initializable<HttpResponse> {
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

	public HttpResponse withBodyGzipCompression() {
		super.setBodyGzipCompression();
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

	public HttpResponse withContentType(MediaType mime, Charset charset) {
		setContentType(mime, charset);
		return this;
	}

	public HttpResponse withContentType(MediaType mime, String charset) {
		return withContentType(mime, Charset.forName(charset));
	}

	public HttpResponse withContentType(MediaType mime) {
		setContentType(mime);
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
		addCookies(cookies);
		return this;
	}

	public HttpResponse withCookies(HttpCookie... cookies) {
		addCookies(cookies);
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

	public void setContentType(MediaType mime, Charset charset) {
		setContentType(ContentType.of(mime, charset));
	}

	public void setContentType(MediaType mime) {
		setContentType(ContentType.of(mime));
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

	public void addCookies(List<HttpCookie> cookies) {
		addHeader(ofSetCookies(HttpHeaders.SET_COOKIE, cookies));
	}

	public void addCookies(HttpCookie... cookies) {
		addCookies(Arrays.asList(cookies));
	}

	public void setCookie(HttpCookie cookie) {
		addCookies(Collections.singletonList(cookie));
	}
	// endregion

	// region getters
	public int getCode() {
		assert !isRecycled();
		return code;
	}

	public int getAge() {
		assert !isRecycled();
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeaderValue(AGE);
		if (header != null)
			try {
				return ByteBufStrings.decodeDecimal(header.array, header.offset, header.size);
			} catch (ParseException e) {
				return 0;
			}
		return 0;
	}

	public Date getExpires() {
		assert !isRecycled();
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeaderValue(EXPIRES);
		if (header != null)
			try {
				return new Date(HttpDate.parse(header.array, header.offset));
			} catch (ParseException e) {
				return null;
			}
		return null;
	}

	public Date getLastModified() {
		assert !isRecycled();
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeaderValue(LAST_MODIFIED);
		if (header != null)
			try {
				return new Date(HttpDate.parse(header.array, header.offset));
			} catch (ParseException e) {
				return null;
			}
		return null;
	}

	@Override
	public List<HttpCookie> getCookies() {
		assert !isRecycled();
		List<HttpCookie> cookies = new ArrayList<>();
		List<Value> headers = getHeaderValues(SET_COOKIE);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			try {
				HttpCookie.parse(value.array, value.offset, value.offset + value.size, cookies);
			} catch (ParseException e) {
				return Collections.emptyList();
			}
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

	@SuppressWarnings("unchecked")
	protected Stage<HttpResponse> ensureBody() {
		return (Stage<HttpResponse>) doEnsureBody();
	}

	@Override
	protected int estimateSize() {
		return estimateSize(LONGEST_FIRST_LINE_SIZE);
	}

	@Override
	protected void writeTo(ByteBuf buf) {
		writeCodeMessage(buf, code);
		writeHeaders(buf);
	}

	@Override
	public String toString() {
		return HttpResponse.class.getSimpleName() + ": " + code;
	}
	// endregion
}
