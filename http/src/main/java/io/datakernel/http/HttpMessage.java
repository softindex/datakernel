/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.annotation.NotNull;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpHeaderValue.HttpHeaderValueOfBuf;
import io.datakernel.http.HttpHeaderValue.ParserIntoList;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.util.MemSize;
import io.datakernel.util.ParserFunction;

import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.util.Preconditions.*;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Represents any HTTP message. Its internal byte buffers will be automatically recycled in HTTP client or HTTP server.
 */
@SuppressWarnings("unused")
public abstract class HttpMessage {
	protected Map<HttpHeader, HttpHeaderValue> headers = new LinkedHashMap<>();
	protected Map<String, HttpCookie> cookies;
	protected SerialSupplier<ByteBuf> bodySupplier;
	protected ByteBuf body;
	protected boolean useGzip;

	protected HttpMessage() {
	}

	public final Map<HttpHeader, HttpHeaderValue> getHeaders() {
		return headers;
	}

	void addHeader(HttpHeader header, ByteBuf buf) {
		assert !isRecycled();
		HttpHeaderValueOfBuf headerBytes =
				(HttpHeaderValueOfBuf) headers.computeIfAbsent(header,
						$ -> new HttpHeaderValueOfBuf());
		headerBytes.add(buf);
	}

	public void setHeader(HttpHeader header, String string) {
		assert !isRecycled();
		setHeader(header, HttpHeaderValue.of(string));
	}

	public void setHeader(HttpHeader header, byte[] value) {
		assert !isRecycled();
		setHeader(header, HttpHeaderValue.ofBytes(value, 0, value.length));
	}

	public void setHeader(HttpHeader header, HttpHeaderValue value) {
		assert !isRecycled();
		HttpHeaderValue prev = headers.put(header, value);
		checkArgument(prev == null);
	}

	@NotNull
	public ByteBuf getHeaderBuf(HttpHeader header) throws ParseException {
		HttpHeaderValueOfBuf headerBuf = (HttpHeaderValueOfBuf) headers.get(header);
		if (headerBuf != null) {
			if (headerBuf.bufs != null) {
				throw new ParseException(HttpMessage.class, "Header '" + header + "' has multiple values");
			}
			return headerBuf.buf;
		}
		throw new ParseException(HttpMessage.class, "There is no header: " + header);
	}

	@Nullable
	public ByteBuf getHeaderBufOrNull(HttpHeader header) throws ParseException {
		HttpHeaderValueOfBuf headerBuf = (HttpHeaderValueOfBuf) headers.get(header);
		if (headerBuf != null) {
			if (headerBuf.bufs != null) {
				throw new ParseException(HttpMessage.class, "Header '" + header + "' has multiple values");
			}
			return headerBuf.buf;
		}
		return null;
	}

	public List<ByteBuf> getHeaderBufs(HttpHeader header) {
		HttpHeaderValueOfBuf headerBuf = (HttpHeaderValueOfBuf) headers.get(header);
		if (headerBuf == null) return emptyList();
		if (headerBuf.bufs == null) return singletonList(headerBuf.buf);
		return Arrays.asList(headerBuf.bufs);
	}

	@NotNull
	public final String getHeader(HttpHeader header) throws ParseException {
		HttpHeaderValue headerValue = headers.get(header);
		if (headerValue != null) return headerValue.toString();
		throw new ParseException(HttpMessage.class, "There is no header: " + header);
	}

	@Nullable
	public final String getHeaderOrNull(HttpHeader header) {
		HttpHeaderValue headerValue = headers.get(header);
		if (headerValue != null) return headerValue.toString();
		return null;
	}

	@NotNull
	public <T> T parseHeader(HttpHeader header, ParserFunction<ByteBuf, T> parser) throws ParseException {
		return parser.parse(getHeaderBuf(header));
	}

	public <T> T parseHeader(HttpHeader header, ParserFunction<ByteBuf, T> parser, T defaultValue) throws ParseException {
		return parser.parseOrDefault(getHeaderBufOrNull(header), defaultValue);
	}

	public <T> List<T> parseHeader(HttpHeader header, ParserIntoList<T> parser) throws ParseException {
		HttpHeaderValueOfBuf headerBuf = (HttpHeaderValueOfBuf) headers.get(header);
		if (headerBuf == null) return emptyList();
		List<T> list = new ArrayList<>();
		if (headerBuf.bufs == null) {
			parser.parse(headerBuf.buf, list);
		} else {
			for (ByteBuf buf : headerBuf.bufs) {
				parser.parse(buf, list);
			}
		}
		return list;
	}

	protected abstract List<HttpCookie> doParseCookies() throws ParseException;

	public abstract void setCookies(List<HttpCookie> cookies);

	public void setCookies(HttpCookie... cookie) {
		setCookies(Arrays.asList(cookie));
	}

	public void setCookie(HttpCookie cookie) {
		setCookies(Collections.singletonList(cookie));
	}

	public Map<String, HttpCookie> getCookies() throws ParseException {
		if (cookies != null) return cookies;
		Map<String, HttpCookie> cookies = new LinkedHashMap<>();
		for (HttpCookie cookie : doParseCookies()) {
			cookies.put(cookie.getName(), cookie);
		}
		this.cookies = cookies;
		return cookies;
	}

	public HttpCookie getCookie(String cookie) throws ParseException {
		HttpCookie httpCookie = getCookies().get(cookie);
		if (httpCookie != null) return httpCookie;
		throw new ParseException(HttpMessage.class, "There is no cookie: " + cookie);
	}

	@Nullable
	public HttpCookie getCookieOrNull(String cookie) throws ParseException {
		HttpCookie httpCookie = getCookies().get(cookie);
		if (httpCookie != null) return httpCookie;
		return null;
	}

	public void setBodyGzipCompression() {
		this.useGzip = true;
	}

	public ByteBuf getBody() {
		ByteBuf body = checkNotNull(this.body);
		this.body = null;
		return body;
	}

	public Promise<ByteBuf> getBodyPromise(MemSize maxBodySize) {
		return getBodyPromise(maxBodySize.toInt());
	}

	public Promise<ByteBuf> getBodyPromise(int maxBodySize) {
		checkState(body != null ^ bodySupplier != null);
		if (body != null) {
			ByteBuf body = this.body;
			this.body = null;
			return Promise.of(body);
		}
		SerialSupplier<ByteBuf> bodySupplier = this.bodySupplier;
		this.bodySupplier = null;
		return bodySupplier.toCollector(ByteBufQueue.collector(maxBodySize));
	}

	public final Promise<Void> ensureBody(MemSize maxBodySize) {
		return ensureBody(maxBodySize.toInt());
	}

	public final Promise<Void> ensureBody(int maxBodySize) {
		if (body != null) return Promise.of(null);
		SerialSupplier<ByteBuf> bodySupplier = this.bodySupplier;
		if (bodySupplier != null) {
			this.bodySupplier = null;
			return bodySupplier.toCollector(ByteBufQueue.collector(maxBodySize))
					.thenComposeEx((buf, e) -> {
						if (e == null) {
							this.body = buf;
							return Promise.of(null);
						} else {
							return Promise.ofException(e);
						}
					});
		}
		return Promise.of(null);
	}

	public SerialSupplier<ByteBuf> getBodyStream() {
		checkState(body != null || bodySupplier != null);
		if (body != null) {
			ByteBuf body = this.body;
			this.body = null;
			return SerialSupplier.of(body);
		}
		SerialSupplier<ByteBuf> bodySupplier = this.bodySupplier;
		this.bodySupplier = null;
		return bodySupplier;
	}

	public void setBody(ByteBuf body) {
		this.body = body;
	}

	public void setBody(byte[] body) {
		setBody(ByteBuf.wrapForReading(body));
	}

	public void setBodyStream(SerialSupplier<ByteBuf> bodySupplier) {
		this.bodySupplier = bodySupplier;
	}

	protected boolean isRecycled() {
		return headers == null;
	}

	/**
	 * Recycles body and header. You should do it before reusing.
	 */
	protected void recycleHeaders() {
		assert !isRecycled();
		for (HttpHeaderValue headerValue : headers.values()) {
			headerValue.recycle();
		}
		headers = null;
	}

	protected void recycle() {
		recycleHeaders();
		if (bodySupplier != null) {
//			bodySupplier.cancel();
			bodySupplier = null;
		}
		if (body != null) {
			body.recycle();
			body = null;
		}
	}

	/**
	 * Sets headers for this message from ByteBuf
	 *
	 * @param buf the new headers
	 */
	protected void writeHeaders(ByteBuf buf) {
		assert !isRecycled();
		for (Map.Entry<HttpHeader, HttpHeaderValue> entry : this.headers.entrySet()) {
			HttpHeader header = entry.getKey();

			buf.put(CR);
			buf.put(LF);
			header.writeTo(buf);
			buf.put((byte) ':');
			buf.put(SP);
			entry.getValue().writeTo(buf);
		}

		buf.put(CR);
		buf.put(LF);
		buf.put(CR);
		buf.put(LF);
	}

	protected int estimateSize(int firstLineSize) {
		assert !isRecycled();
		int size = firstLineSize;
		for (Map.Entry<HttpHeader, HttpHeaderValue> entry : this.headers.entrySet()) {
			HttpHeader header = entry.getKey();
			size += 2 + header.size() + 2 + entry.getValue().estimateSize(); // CR,LF,header,": ",value
		}
		size += 4; // CR,LF,CR,LF
		return size;
	}

	protected abstract int estimateSize();

	protected abstract void writeTo(ByteBuf buf);
}
