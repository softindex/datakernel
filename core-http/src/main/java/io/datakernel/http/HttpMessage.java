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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.exception.InvalidSizeException;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.HttpHeaderValue.ParserIntoList;
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.MemSize;
import io.datakernel.util.ParserFunction;
import io.datakernel.util.Recyclable;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static java.util.Arrays.copyOf;

/**
 * Represents any HTTP message. Its internal byte buffers will be automatically recycled in HTTP client or HTTP server.
 */
@SuppressWarnings({"unused", "WeakerAccess", "PointlessBitwiseExpression"})
public abstract class HttpMessage {
	final HttpHeadersMultimap<HttpHeader, HttpHeaderValue> headers = new HttpHeadersMultimap<>();
	ChannelSupplier<ByteBuf> bodySupplier;
	private Recyclable bufs;

	static final byte ACCESSED_BODY_STREAM = 1 << 0;
	static final byte USE_GZIP = 1 << 1;
	static final byte RECYCLED = (byte) (1 << 7);

	@MagicConstant(flags = {ACCESSED_BODY_STREAM, USE_GZIP, RECYCLED})
	byte flags;

	static final int DEFAULT_LOAD_LIMIT_BYTES =
			ApplicationSettings.getInt(HttpMessage.class, "loadLimit", 1024 * 1024 * 1024);

	public static final MemSize DEFAULT_LOAD_LIMIT = MemSize.of(DEFAULT_LOAD_LIMIT_BYTES);

	protected HttpMessage() {
	}

	void addHeaderBuf(@NotNull ByteBuf buf) {
		buf.addRef();
		if (bufs == null) {
			bufs = buf;
		} else {
			Recyclable prev = this.bufs;
			this.bufs = () -> {
				prev.recycle();
				buf.recycle();
			};
		}
	}

	void addParsedHeader(@NotNull HttpHeader header, @NotNull byte[] array, int off, int len) {
		assert !isRecycled();
		headers.add(header, HttpHeaderValue.ofBytes(array, off, len));
	}

	public void addHeader(@NotNull HttpHeader header, @NotNull String string) {
		assert !isRecycled();
		addHeader(header, HttpHeaderValue.of(string));
	}

	public void addHeader(@NotNull HttpHeader header, @NotNull byte[] value) {
		assert !isRecycled();
		addHeader(header, HttpHeaderValue.ofBytes(value, 0, value.length));
	}

	public void addHeader(@NotNull HttpHeader header, @NotNull HttpHeaderValue value) {
		assert !isRecycled();
		headers.add(header, value);
	}

	@NotNull
	private ByteBuf getHeaderBuf(@NotNull HttpHeader header) throws ParseException {
		HttpHeaderValue headerBuf = headers.get(header);
		if (headerBuf != null) {
			return headerBuf.getBuf();
		}
		throw new ParseException(HttpMessage.class, "There is no header: " + header);
	}

	@Nullable
	private ByteBuf getHeaderBufOrNull(@NotNull HttpHeader header) {
		HttpHeaderValue headerBuf = headers.get(header);
		return headerBuf != null ? headerBuf.getBuf() : null;
	}

	@NotNull
	public final Map<HttpHeader, String[]> getHeaders() {
		LinkedHashMap<HttpHeader, String[]> map = new LinkedHashMap<>(headers.size() * 2);
		for (int i = 0; i != headers.kvPairs.length; i += 2) {
			HttpHeader k = (HttpHeader) headers.kvPairs[i];
			if (k != null) {
				HttpHeaderValue v = (HttpHeaderValue) headers.kvPairs[i + 1];
				map.compute(k, ($, strings) -> {
					String headerString = v.toString();
					if (strings == null) return new String[]{headerString};
					String[] newStrings = copyOf(strings, strings.length + 1);
					newStrings[newStrings.length - 1] = headerString;
					return newStrings;
				});
			}
		}
		return map;
	}

	@NotNull
	public final String getHeader(@NotNull HttpHeader header) throws ParseException {
		HttpHeaderValue headerValue = headers.get(header);
		if (headerValue != null) return headerValue.toString();
		throw new ParseException(HttpMessage.class, "There is no header: " + header);
	}

	@Nullable
	public final String getHeaderOrNull(@NotNull HttpHeader header) {
		HttpHeaderValue headerValue = headers.get(header);
		if (headerValue != null) return headerValue.toString();
		return null;
	}

	@NotNull
	public <T> T parseHeader(@NotNull HttpHeader header, @NotNull ParserFunction<ByteBuf, T> parser) throws ParseException {
		return parser.parse(getHeaderBuf(header));
	}

	public <T> T parseHeader(@NotNull HttpHeader header, @NotNull ParserFunction<ByteBuf, T> parser, @Nullable T defaultValue) throws ParseException {
		return parser.parseOrDefault(getHeaderBufOrNull(header), defaultValue);
	}

	@NotNull
	public <T> List<T> parseHeader(@NotNull HttpHeader header, @NotNull ParserIntoList<T> parser) throws ParseException {
		List<T> list = new ArrayList<>();
		for (int i = header.hashCode() & (headers.kvPairs.length - 2); ; i = (i + 2) & (headers.kvPairs.length - 2)) {
			HttpHeader k = (HttpHeader) headers.kvPairs[i];
			if (k == null) {
				break;
			}
			if (k.equals(header)) {
				parser.parse(((HttpHeaderValue) headers.kvPairs[i + 1]).getBuf(), list);
			}
		}
		return list;
	}

	public abstract void addCookies(@NotNull List<HttpCookie> cookies);

	public void addCookies(@NotNull HttpCookie... cookies) {
		ArrayList<HttpCookie> list = new ArrayList<>(cookies.length);
		Collections.addAll(list, cookies);
		addCookies(list);
	}

	public void addCookie(@NotNull HttpCookie cookie) {
		ArrayList<HttpCookie> list = new ArrayList<>(1);
		list.add(cookie);
		addCookies(list);
	}

	public void setBodyStream(@NotNull ChannelSupplier<ByteBuf> bodySupplier) {
		this.bodySupplier = bodySupplier;
	}

	public ChannelSupplier<ByteBuf> getBodyStream() {
		flags |= ACCESSED_BODY_STREAM;
		return this.bodySupplier;
	}

	public void setBody(@NotNull ByteBuf body) {
		this.bodySupplier = ChannelSupplier.of(body);
	}

	public void setBody(@NotNull byte[] body) {
		setBody(ByteBuf.wrapForReading(body));
	}

	@NotNull
	public final Promise<ByteBuf> getBody() {
		return getBody(DEFAULT_LOAD_LIMIT_BYTES);
	}

	@NotNull
	public final Promise<ByteBuf> getBody(@NotNull MemSize loadLimit) {
		return getBody(loadLimit.toInt());
	}

	@NotNull
	public final Promise<ByteBuf> getBody(int loadLimit) {
		if (this.bodySupplier instanceof ChannelSuppliers.ChannelSupplierOfValue<?>) {
			flags |= ACCESSED_BODY_STREAM;
			return Promise.of(((ChannelSuppliers.ChannelSupplierOfValue<ByteBuf>) bodySupplier).getValue());
		}
		return ChannelSuppliers.collect(getBodyStream(),
				new ByteBufQueue(),
				(queue, buf) -> {
					if (queue.hasRemainingBytes(loadLimit)) {
						queue.recycle();
						buf.recycle();
						throw new UncheckedException(new InvalidSizeException(HttpMessage.class,
								"HTTP body size exceeds load limit " + loadLimit));
					}
					queue.add(buf);
				},
				ByteBufQueue::takeRemaining);
	}

	public void setBodyGzipCompression() {
		this.flags |= USE_GZIP;
	}

	boolean isRecycled() {
		return (this.flags & RECYCLED) != 0;
	}

	@SuppressWarnings("AssertWithSideEffects")
	final void recycle() {
		assert !isRecycled();
		assert (this.flags |= RECYCLED) != 0;
		if (bufs != null) {
			bufs.recycle();
		}
	}

	/**
	 * Sets headers for this message from ByteBuf
	 *
	 * @param buf the new headers
	 */
	protected void writeHeaders(@NotNull ByteBuf buf) {
		assert !isRecycled();
		for (int i = 0; i != headers.kvPairs.length; i += 2) {
			HttpHeader k = (HttpHeader) headers.kvPairs[i];
			if (k != null) {
				HttpHeaderValue v = (HttpHeaderValue) headers.kvPairs[i + 1];
				buf.put(CR);
				buf.put(LF);
				k.writeTo(buf);
				buf.put((byte) ':');
				buf.put(SP);
				v.writeTo(buf);
			}
		}
		buf.put(CR);
		buf.put(LF);
		buf.put(CR);
		buf.put(LF);
	}

	protected int estimateSize(int firstLineSize) {
		assert !isRecycled();
		int size = firstLineSize;
		// CR,LF,header,": ",value
		for (int i = 0; i != headers.kvPairs.length; i += 2) {
			HttpHeader k = (HttpHeader) headers.kvPairs[i];
			if (k != null) {
				HttpHeaderValue v = (HttpHeaderValue) headers.kvPairs[i + 1];
				// CR,LF,header,": ",value
				size += 2 + k.size() + 2 + v.estimateSize();
			}
		}
		size += 4; // CR,LF,CR,LF
		return size;
	}

	protected abstract int estimateSize();

	protected abstract void writeTo(@NotNull ByteBuf buf);
}
