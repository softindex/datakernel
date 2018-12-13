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
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpHeaderValue.ParsedHttpHeaderValue;
import io.datakernel.http.HttpHeaderValue.ParserIntoList;
import io.datakernel.util.MemSize;
import io.datakernel.util.ParserFunction;

import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Collections.emptyList;

/**
 * Represents any HTTP message. Its internal byte buffers will be automatically recycled in HTTP client or HTTP server.
 */
@SuppressWarnings("unused")
public abstract class HttpMessage {
	protected Map<HttpHeader, HttpHeaderValue> headers = new LinkedHashMap<>();
	protected ChannelSupplier<ByteBuf> bodySupplier;
	protected ByteBuf body;
	protected boolean useGzip;

	protected HttpMessage() {
	}

	void addParsedHeader(HttpHeader header, ByteBuf buf) {
		assert !isRecycled();
		ParsedHttpHeaderValue headerBytes =
				(ParsedHttpHeaderValue) headers.computeIfAbsent(header,
						$ -> new ParsedHttpHeaderValue());
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
		checkArgument(prev == null, "Header '%s' has already been set", header);
	}

	@NotNull
	private ByteBuf getHeaderBuf(HttpHeader header) throws ParseException {
		ParsedHttpHeaderValue headerBuf = (ParsedHttpHeaderValue) headers.get(header);
		if (headerBuf != null) {
			return headerBuf.buf;
		}
		throw new ParseException(HttpMessage.class, "There is no header: " + header);
	}

	@Nullable
	private ByteBuf getHeaderBufOrNull(HttpHeader header) {
		ParsedHttpHeaderValue headerBuf = (ParsedHttpHeaderValue) headers.get(header);
		return headerBuf != null ? headerBuf.buf : null;
	}

	public final Map<HttpHeader, String[]> getHeaders() {
		LinkedHashMap<HttpHeader, String[]> map = new LinkedHashMap<>(headers.size() * 3 / 2);
		for (Map.Entry<HttpHeader, HttpHeaderValue> entry : headers.entrySet()) {
			map.put(entry.getKey(), ((ParsedHttpHeaderValue) entry.getValue()).toStrings());
		}
		return map;
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
		ParsedHttpHeaderValue headerBuf = (ParsedHttpHeaderValue) headers.get(header);
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

	public abstract void setCookies(List<HttpCookie> cookies);

	public void setCookies(HttpCookie... cookie) {
		setCookies(Arrays.asList(cookie));
	}

	public void setCookie(HttpCookie cookie) {
		setCookies(Collections.singletonList(cookie));
	}

	public void setBodyGzipCompression() {
		this.useGzip = true;
	}

	public final ByteBuf getBody() {
		return this.body;
	}

	public final ByteBuf takeBody() {
		ByteBuf body = this.body;
		this.body = null;
		return body;
	}

	protected final Promise<? extends HttpMessage> doEnsureBody(MemSize maxBodySize) {
		return doEnsureBody(maxBodySize.toInt());
	}

	protected final Promise<? extends HttpMessage> doEnsureBody(int maxBodySize) {
		if (body != null) return Promise.of(this);
		if (bodySupplier != null) {
			ChannelSupplier<ByteBuf> bodySupplier = this.bodySupplier;
			this.bodySupplier = null;
			return bodySupplier.toCollector(ByteBufQueue.collector(maxBodySize))
					.thenComposeEx((buf, e) -> {
						if (e == null) {
							this.body = buf;
							return Promise.of(this);
						} else {
							return Promise.ofException(e);
						}
					});
		}
		this.body = ByteBuf.empty();
		return Promise.of(this);
	}

	public ChannelSupplier<ByteBuf> getBodyStream() {
		checkState(body != null || bodySupplier != null, "Either body or body supplier should be present");
		if (body != null) {
			return ChannelSupplier.of(body.slice());
		}
		ChannelSupplier<ByteBuf> bodySupplier = this.bodySupplier;
		this.bodySupplier = null;
		return bodySupplier;
	}

	public void setBody(ByteBuf body) {
		this.body = body;
	}

	public void setBody(byte[] body) {
		setBody(ByteBuf.wrapForReading(body));
	}

	public void setBodyStream(ChannelSupplier<ByteBuf> bodySupplier) {
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
		for (Map.Entry<HttpHeader, HttpHeaderValue> entry : headers.entrySet()) {
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
		for (Map.Entry<HttpHeader, HttpHeaderValue> entry : headers.entrySet()) {
			HttpHeader header = entry.getKey();
			size += 2 + header.size() + 2 + entry.getValue().estimateSize(); // CR,LF,header,": ",value
		}
		size += 4; // CR,LF,CR,LF
		return size;
	}

	protected abstract int estimateSize();

	protected abstract void writeTo(ByteBuf buf);
}
