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
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.MemSize;
import io.datakernel.util.ParserFunction;

import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static java.util.Arrays.copyOf;

/**
 * Represents any HTTP message. Its internal byte buffers will be automatically recycled in HTTP client or HTTP server.
 */
@SuppressWarnings("unused")
public abstract class HttpMessage {
	protected final HttpHeadersMultimap<HttpHeader, HttpHeaderValue> headers = new HttpHeadersMultimap<>();
	protected ChannelSupplier<ByteBuf> bodySupplier;

	byte flags;
	static final byte DETACHED_BODY_STREAM = 1 << 0;
	static final byte USE_GZIP = 1 << 1;
	static final byte RECYCLED = (byte) (1 << 7);

	public static final MemSize DEFAULT_MAX_BODY_SIZE = MemSize.of(
			ApplicationSettings.getInt(HttpMessage.class, "maxBodySize", 1024 * 1024));

	protected static final int DEFAULT_MAX_BODY_SIZE_BYTES = DEFAULT_MAX_BODY_SIZE.toInt();

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

	public void setBodyStream(ChannelSupplier<ByteBuf> bodySupplier) {
		this.bodySupplier = bodySupplier;
	}

	public ChannelSupplier<ByteBuf> getBodyStream() {
		flags |= DETACHED_BODY_STREAM;
		return this.bodySupplier;
	}

	public void setBody(ByteBuf body) {
		this.bodySupplier = ChannelSupplier.of(body);
	}

	public void setBody(byte[] body) {
		setBody(ByteBuf.wrapForReading(body));
	}

	public final Promise<ByteBuf> getBody() {
		return getBody(DEFAULT_MAX_BODY_SIZE_BYTES);
	}

	public final Promise<ByteBuf> getBody(MemSize maxBodySize) {
		return getBody(maxBodySize.toInt());
	}

	public final Promise<ByteBuf> getBody(int maxBodySize) {
		return getBodyStream().toCollector(ByteBufQueue.collector(maxBodySize));
	}

	public void setBodyGzipCompression() {
		this.flags |= USE_GZIP;
	}

	boolean isRecycled() {
		return (this.flags & RECYCLED) != 0;
	}

	final void recycle() {
		assert !isRecycled();
		assert (this.flags |= RECYCLED) != 0;
		headers.forEach((httpHeader, httpHeaderValue) -> httpHeaderValue.recycle());
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