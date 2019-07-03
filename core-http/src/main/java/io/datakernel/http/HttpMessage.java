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
import io.datakernel.util.MemSize;
import io.datakernel.util.ParserFunction;
import io.datakernel.util.Recyclable;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.csp.ChannelConsumers.recycling;

/**
 * Represents any HTTP message. Its internal byte buffers will be automatically recycled in HTTP client or HTTP server.
 */
@SuppressWarnings({"unused", "WeakerAccess", "PointlessBitwiseExpression"})
public abstract class HttpMessage {
	static final byte MUST_LOAD_BODY = 1 << 0;
	static final byte USE_GZIP = 1 << 1;
	static final byte RECYCLED = (byte) (1 << 7);

	@MagicConstant(flags = {MUST_LOAD_BODY, USE_GZIP, RECYCLED})
	byte flags;

	final HttpHeadersMultimap<HttpHeader, HttpHeaderValue> headers = new HttpHeadersMultimap<>();
	ByteBuf body;
	ChannelSupplier<ByteBuf> bodyStream;
	Recyclable bufs;

	protected int maxBodySize;
	protected Map<Type, Object> attachments;

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

	public void addHeader(@NotNull HttpHeader header, @NotNull String string) {
		assert !isRecycled();
		addHeader(header, HttpHeaderValue.of(string));
	}

	public void addHeader(@NotNull HttpHeader header, @NotNull byte[] value) {
		assert !isRecycled();
		addHeader(header, HttpHeaderValue.ofBytes(value, 0, value.length));
	}

	public void addHeader(@NotNull HttpHeader header, @NotNull byte[] array, int off, int len) {
		assert !isRecycled();
		headers.add(header, HttpHeaderValue.ofBytes(array, off, len));
	}

	public void addHeader(@NotNull HttpHeader header, @NotNull HttpHeaderValue value) {
		assert !isRecycled();
		headers.add(header, value);
	}

	public final Collection<Map.Entry<HttpHeader, HttpHeaderValue>> getHeaders() {
		return headers.getEntries();
	}

	@NotNull
	public final <T> List<T> getHeader(@NotNull HttpHeader header, @NotNull ParserIntoList<T> parser) {
		List<T> list = new ArrayList<>();
		for (int i = header.hashCode() & (headers.kvPairs.length - 2); ; i = (i + 2) & (headers.kvPairs.length - 2)) {
			HttpHeader k = (HttpHeader) headers.kvPairs[i];
			if (k == null) {
				break;
			}
			if (k.equals(header)) {
				try {
					parser.parse(((HttpHeaderValue) headers.kvPairs[i + 1]).getBuf(), list);
				} catch (ParseException ignored) {
				}
			}
		}
		return list;
	}

	@Nullable
	public <T> T getHeader(HttpHeader contentType, ParserFunction<ByteBuf, T> parser) {
		return parser.parseOrDefault(getHeaderBuf(contentType), null);
	}

	@Nullable
	public final String getHeader(@NotNull HttpHeader header) {
		HttpHeaderValue headerValue = headers.get(header);
		return headerValue != null ? headerValue.toString() : null;
	}

	@Nullable
	public final ByteBuf getHeaderBuf(@NotNull HttpHeader header) {
		HttpHeaderValue headerBuf = headers.get(header);
		return headerBuf != null ? headerBuf.getBuf() : null;
	}

	public abstract void addCookies(@NotNull List<HttpCookie> cookies);

	public void addCookies(@NotNull HttpCookie... cookies) {
		addCookies(Arrays.asList(cookies));
	}

	public void addCookie(@NotNull HttpCookie cookie) {
		addCookies(Collections.singletonList(cookie));
	}

	public void setBodyStream(@NotNull ChannelSupplier<ByteBuf> bodySupplier) {
		this.bodyStream = bodySupplier;
	}

	public ChannelSupplier<ByteBuf> getBodyStream() {
		ChannelSupplier<ByteBuf> bodyStream = this.bodyStream;
		this.bodyStream = null;
		if (bodyStream != null) return bodyStream;
		if (body != null) {
			ByteBuf body = this.body;
			this.body = null;
			return ChannelSupplier.of(body);
		}
		throw new IllegalStateException("Body stream is missing or already consumed");
	}

	public void setBody(@NotNull ByteBuf body) {
		this.body = body;
	}

	public void setBody(@NotNull byte[] body) {
		setBody(ByteBuf.wrapForReading(body));
	}

	public final ByteBuf getBody() {
		if ((flags & MUST_LOAD_BODY) != 0) throw new IllegalStateException("Body is not loaded");
		if (body != null) return body;
		throw new IllegalStateException("Body is missing or already consumed");
	}

	public final ByteBuf takeBody() {
		ByteBuf body = getBody();
		this.body = null;
		return body;
	}

	public final boolean isBodyLoaded() {
		return (flags & MUST_LOAD_BODY) == 0 && body != null;
	}

	public void setMaxBodySize(MemSize maxBodySize) {
		this.maxBodySize = maxBodySize.toInt();
	}

	public void setMaxBodySize(int maxBodySize) {
		this.maxBodySize = maxBodySize;
	}

	public Promise<ByteBuf> loadBody() {
		return loadBody(maxBodySize);
	}

	public Promise<ByteBuf> loadBody(@NotNull MemSize maxBodySize) {
		return loadBody(maxBodySize.toInt());
	}

	public Promise<ByteBuf> loadBody(int maxBodySize) {
		if (body != null) {
			this.flags &= ~MUST_LOAD_BODY;
			return Promise.of(body);
		}
		ChannelSupplier<ByteBuf> bodyStream = this.bodyStream;
		if (bodyStream == null) throw new IllegalStateException("Body stream is missing or already consumed");
		this.bodyStream = null;
		return ChannelSuppliers.collect(bodyStream,
				new ByteBufQueue(),
				(queue, buf) -> {
					if (maxBodySize != 0 && queue.hasRemainingBytes(maxBodySize)) {
						queue.recycle();
						buf.recycle();
						throw new UncheckedException(new InvalidSizeException(HttpMessage.class,
								"HTTP body size exceeds load limit " + maxBodySize));
					}
					queue.add(buf);
				},
				ByteBufQueue::takeRemaining)
				.whenComplete((body, e) -> {
					if (!isRecycled()) {
						this.flags &= ~MUST_LOAD_BODY;
						this.body = body;
					} else {
						body.recycle();
					}
				});
	}

	public <T> void attach(Type type, T extra) {
		if (attachments == null) {
			attachments = new HashMap<>();
		}
		attachments.put(type, extra);
	}

	public <T> void attach(Class<T> type, T extra) {
		if (attachments == null) {
			attachments = new HashMap<>();
		}
		attachments.put(type, extra);
	}

	public void attach(Object extra) {
		if (attachments == null) {
			attachments = new HashMap<>();
		}
		attachments.put(extra.getClass(), extra);
	}

	@SuppressWarnings("unchecked")
	public <T> T getAttachment(Class<T> type) {
		if (attachments == null) {
			return null;
		}
		Object res = attachments.get(type);
		return (T) res;
	}

	@SuppressWarnings("unchecked")
	public <T> T getAttachment(Type type) {
		if (attachments == null) {
			return null;
		}
		Object res = attachments.get(type);
		return (T) res;
	}

	public void setBodyGzipCompression() {
		this.flags |= USE_GZIP;
	}

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	boolean isRecycled() {
		return (this.flags & RECYCLED) != 0;
	}

	final void recycle() {
		assert !isRecycled();
		if (bufs != null) {
			bufs.recycle();
		}
		if (body != null) {
			body.recycle();
		}
		if (bodyStream != null) {
			bodyStream.streamTo(recycling());
		}
	}

	/**
	 * Sets headers for this message from ByteBuf
	 *
	 * @param buf the new headers
	 */
	protected void writeHeaders(@NotNull ByteBuf buf) {
		assert !isRecycled();
		for (int i = 0; i < headers.kvPairs.length - 1; i += 2) {
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
		for (int i = 0; i < headers.kvPairs.length - 1; i += 2) {
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
