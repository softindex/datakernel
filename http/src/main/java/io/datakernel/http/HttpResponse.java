/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.datakernel.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.util.Initializable;

import java.util.List;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.putDecimal;
import static io.datakernel.http.HttpHeaders.LOCATION;
import static io.datakernel.http.HttpHeaders.SET_COOKIE;

/**
 * Represents HTTP response for {@link HttpRequest}. After handling {@code HttpResponse} will be recycled so you cannot
 * usi it afterwards.
 */
public final class HttpResponse extends HttpMessage implements Initializable<HttpResponse> {
	// region internal
	private static final byte[] HTTP11_BYTES = encodeAscii("HTTP/1.1 ");
	private static final byte[] CODE_ERROR_BYTES = encodeAscii(" Error");
	private static final byte[] CODE_OK_BYTES = encodeAscii(" OK");
	private static final byte[] CODE_200_BYTES = encodeAscii("HTTP/1.1 200 OK");
	private static final byte[] CODE_302_BYTES = encodeAscii("HTTP/1.1 302 Found");
	private static final byte[] CODE_400_BYTES = encodeAscii("HTTP/1.1 400 Bad Request");
	private static final byte[] CODE_403_BYTES = encodeAscii("HTTP/1.1 403 Forbidden");
	private static final byte[] CODE_404_BYTES = encodeAscii("HTTP/1.1 404 Not Found");
	private static final byte[] CODE_500_BYTES = encodeAscii("HTTP/1.1 500 Internal Server Error");
	private static final byte[] CODE_503_BYTES = encodeAscii("HTTP/1.1 503 Service Unavailable");
	private static final int LONGEST_FIRST_LINE_SIZE = CODE_500_BYTES.length;

	private final int code;

	// region creators
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
		response.setHeader(LOCATION, url);
		return response;
	}

	// common builder methods
	public HttpResponse withHeader(HttpHeader header, String value) {
		setHeader(header, value);
		return this;
	}

	public HttpResponse withHeader(HttpHeader header, byte[] bytes) {
		setHeader(header, bytes);
		return this;
	}

	public HttpResponse withHeader(HttpHeader header, HttpHeaderValue value) {
		setHeader(header, value);
		return this;
	}

	@Override
	protected List<HttpCookie> doParseCookies() throws ParseException {
		return parseHeader(SET_COOKIE, HttpHeaderValue::toFullCookies);
	}

	@Override
	public void setCookies(List<HttpCookie> cookies) {
		setHeader(SET_COOKIE, new HttpHeaderValue.HttpHeaderValueOfFullCookies(cookies));
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

	public HttpResponse withBodyStream(SerialSupplier<ByteBuf> stream) {
		setBodyStream(stream);
		return this;
	}

	// endregion

	public int getCode() {
		assert !isRecycled();
		return code;
	}

	private static void writeCodeMessageEx(ByteBuf buf, int code) {
		buf.put(HTTP11_BYTES);
		putDecimal(buf, code);
		if (code >= 400) {
			buf.put(CODE_ERROR_BYTES);
		} else {
			buf.put(CODE_OK_BYTES);
		}
	}

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
