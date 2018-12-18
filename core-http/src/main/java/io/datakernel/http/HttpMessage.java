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
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.HttpHeaderValue.HttpHeaderValueOfBuf;
import io.datakernel.http.HttpHeaderValue.ParserIntoList;
import io.datakernel.util.MemSize;
import io.datakernel.util.ParserFunction;

import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Arrays.copyOf;

/**
 * Represents any HTTP message. Its internal byte buffers will be automatically recycled in HTTP client or HTTP server.
 */
@SuppressWarnings("unused")
public abstract class HttpMessage {
	protected HttpHeadersMultimap<HttpHeader, HttpHeaderValue> headers = new HttpHeadersMultimap<>();
	protected ChannelSupplier<ByteBuf> bodySupplier;
	protected ByteBuf body;
	protected boolean useGzip;

	protected HttpMessage() {
	}

	void addParsedHeader(HttpHeader header, ByteBuf buf) {
		assert !isRecycled();
		headers.add(header, new HttpHeaderValueOfBuf(buf));
	}

	public void addHeader(HttpHeader header, String string) {
		assert !isRecycled();
		addHeader(header, HttpHeaderValue.of(string));
	}

	public void addHeader(HttpHeader header, byte[] value) {
		assert !isRecycled();
		addHeader(header, HttpHeaderValue.ofBytes(value, 0, value.length));
	}

	public void addHeader(HttpHeader header, HttpHeaderValue value) {
		assert !isRecycled();
		headers.add(header, value);
	}

	@NotNull
	private ByteBuf getHeaderBuf(HttpHeader header) throws ParseException {
		HttpHeaderValue headerBuf = headers.get(header);
		if (headerBuf != null) {
			return headerBuf.getBuf();
		}
		throw new ParseException(HttpMessage.class, "There is no header: " + header);
	}

	@Nullable
	private ByteBuf getHeaderBufOrNull(HttpHeader header) {
		HttpHeaderValue headerBuf = headers.get(header);
		return headerBuf != null ? headerBuf.getBuf() : null;
	}

	public final Map<HttpHeader, String[]> getHeaders() {
		LinkedHashMap<HttpHeader, String[]> map = new LinkedHashMap<>(headers.size() * 2);
		headers.forEach((httpHeader, httpHeaderValue) ->
				map.compute(httpHeader, ($, strings) -> {
					String headerString = httpHeaderValue.toString();
					if (strings == null) return new String[]{headerString};
					String[] newStrings = copyOf(strings, strings.length + 1);
					newStrings[newStrings.length - 1] = headerString;
					return newStrings;
				}));
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
		try {
			List<T> list = new ArrayList<>();
			headers.forEach(header, httpHeaderValue -> {
				try {
					parser.parse(httpHeaderValue.getBuf(), list);
				} catch (ParseException e) {
					throw new UncheckedException(e);
				}
			});
			return list;
		} catch (UncheckedException u) {
			throw u.propagate(ParseException.class);
		}
	}

	public abstract void addCookies(List<HttpCookie> cookies);

	public void addCookies(HttpCookie... cookies) {
		ArrayList<HttpCookie> list = new ArrayList<>(cookies.length);
		Collections.addAll(list, cookies);
		addCookies(list);
	}

	public void addCookie(HttpCookie cookie) {
		ArrayList<HttpCookie> list = new ArrayList<>(1);
		list.add(cookie);
		addCookies(list);
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
		headers.forEach((httpHeader, httpHeaderValue) ->
				httpHeaderValue.recycle());
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
		headers.forEach((httpHeader, httpHeaderValue) -> {
			buf.put(CR);
			buf.put(LF);
			httpHeader.writeTo(buf);
			buf.put((byte) ':');
			buf.put(SP);
			httpHeaderValue.writeTo(buf);
		});
		buf.put(CR);
		buf.put(LF);
		buf.put(CR);
		buf.put(LF);
	}

	protected int estimateSize(int firstLineSize) {
		assert !isRecycled();
		int size = firstLineSize;
		int[] headersSize = new int[1]; // for stack allocation
		headers.forEach((httpHeader, httpHeaderValue) -> {
			headersSize[0] += 2 + httpHeader.size() + 2 + httpHeaderValue.estimateSize(); // CR,LF,header,": ",value
		});
		size += headersSize[0];
		size += 4; // CR,LF,CR,LF
		return size;
	}

	protected abstract int estimateSize();

	protected abstract void writeTo(ByteBuf buf);
}
