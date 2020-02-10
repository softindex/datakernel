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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.common.MemSize;
import io.datakernel.common.Recyclable;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.common.parse.InvalidSizeException;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.parse.ParserFunction;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.http.HttpHeaderValue.ParserIntoList;
import io.datakernel.promise.Promise;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.csp.ChannelConsumers.recycling;
import static java.util.Collections.emptySet;

/**
 * Represents any HTTP message. Its internal byte buffers will be automatically recycled in HTTP client or HTTP server.
 */
@SuppressWarnings({"unused", "WeakerAccess", "PointlessBitwiseExpression"})
public abstract class HttpMessage {
	/**
	 * This flag means that the body of this message should not be streamed
	 * and should be collected into a single body {@link ByteBuf}.
	 * This flag is removed when body is taken away or recycled.
	 */
	static final byte MUST_LOAD_BODY = 1 << 0;
	/**
	 * This flag means that the DEFLATE compression algorithm will be used
	 * to compress/decompress the body of this message.
	 */
	static final byte USE_GZIP = 1 << 1;

	/**
	 * This flag means that the body was already recycled and is not accessible.
	 * It is mostly used in assertions.
	 */
	static final byte RECYCLED = (byte) (1 << 7);

	@MagicConstant(flags = {MUST_LOAD_BODY, USE_GZIP, RECYCLED})
	byte flags;

	final HttpHeadersMultimap<HttpHeader, HttpHeaderValue> headers = new HttpHeadersMultimap<>();
	@Nullable ByteBuf body;
	@Nullable ChannelSupplier<ByteBuf> bodyStream;
	Recyclable bufs;

	protected int maxBodySize;
	protected Map<Object, Object> attachments;

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
		addHeader(header, HttpHeaderValue.ofBytes(array, off, len));
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
	public <T> T getHeader(HttpHeader header, ParserFunction<ByteBuf, T> parser) {
		return parser.parseOrDefault(getHeaderBuf(header), null);
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

	public void addCookies(@NotNull HttpCookie... cookies) {
		addCookies(Arrays.asList(cookies));
	}

	public abstract void addCookies(@NotNull List<HttpCookie> cookies);

	public abstract void addCookie(@NotNull HttpCookie cookie);

	public void setBodyStream(@NotNull ChannelSupplier<ByteBuf> bodySupplier) {
		this.bodyStream = bodySupplier;
	}

	/**
	 * This method transfers the "rust-like ownership" from this message object
	 * to the caller.
	 * Thus it can be called only once and it it the caller responsibility
	 * to recycle the byte buffers received.
	 */
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

	/**
	 * Allows you to peak at the body when it is available without taking the ownership.
	 */
	public final ByteBuf getBody() {
		if ((flags & MUST_LOAD_BODY) != 0) throw new IllegalStateException("Body is not loaded");
		if (body != null) return body;
		throw new IllegalStateException("Body is missing or already consumed");
	}

	/**
	 * Similarly to {@link #getBodyStream}, this method transfers ownership and can be called only once.
	 * It returns sucessfully only when this message in in {@link #MUST_LOAD_BODY non-streaming mode}
	 */
	public final ByteBuf takeBody() {
		ByteBuf body = getBody();
		this.body = null;
		return body;
	}

	/**
	 * Checks if this message is working in streaming mode or not.
	 * Returns true if not.
	 */
	public final boolean isBodyLoaded() {
		return (flags & MUST_LOAD_BODY) == 0 && body != null;
	}

	public void setMaxBodySize(MemSize maxBodySize) {
		this.maxBodySize = maxBodySize.toInt();
	}

	public void setMaxBodySize(int maxBodySize) {
		this.maxBodySize = maxBodySize;
	}

	/**
	 * @see #loadBody(int)
	 */
	public Promise<ByteBuf> loadBody() {
		return loadBody(maxBodySize);
	}

	/**
	 * @see #loadBody(int)
	 */
	public Promise<ByteBuf> loadBody(@NotNull MemSize maxBodySize) {
		return loadBody(maxBodySize.toInt());
	}

	/**
	 * Consumes the body stream if this message works in {@link #MUST_LOAD_BODY streaming mode} and collects
	 * it to a single {@link ByteBuf} or just returns the body if message is not in streaming mode.
	 *
	 * @param maxBodySize max number of bytes to load from the stream, an exception is returned if exceeded.
	 */
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
				.whenResult(body -> {
					if (!isRecycled()) {
						this.flags &= ~MUST_LOAD_BODY;
						this.body = body;
					} else {
						body.recycle();
					}
				});
	}

	/**
	 * Attaches an arbitrary object to this message by its type.
	 * This is used for context management.
	 * For example some {@link io.datakernel.http.session.SessionServlet wrapper auth servlet} could
	 * add some kind of session data here.
	 */
	public <T> void attach(Type type, T extra) {
		if (attachments == null) {
			attachments = new HashMap<>();
		}
		attachments.put(type, extra);
	}

	/**
	 * @see #attach(Type, Object)
	 */
	public <T> void attach(Class<T> type, T extra) {
		if (attachments == null) {
			attachments = new HashMap<>();
		}
		attachments.put(type, extra);
	}

	/**
	 * @see #attach(Type, Object)
	 */
	public void attach(Object extra) {
		if (attachments == null) {
			attachments = new HashMap<>();
		}
		attachments.put(extra.getClass(), extra);
	}

	/**
	 * Attaches an arbitrary object to this message by string key.
	 * This is used for context management.
	 */
	public <T> void attach(String key, T extra) {
		if (attachments == null) {
			attachments = new HashMap<>();
		}
		attachments.put(key, extra);
	}

	/**
	 * @see #attach(Type, Object)
	 */
	@SuppressWarnings("unchecked")
	public <T> T getAttachment(Class<T> type) {
		if (attachments == null) {
			return null;
		}
		Object res = attachments.get(type);
		return (T) res;
	}

	/**
	 * @see #attach(Type, Object)
	 */
	@SuppressWarnings("unchecked")
	public <T> T getAttachment(Type type) {
		if (attachments == null) {
			return null;
		}
		Object res = attachments.get(type);
		return (T) res;
	}

	/**
	 * @see #attach(String, Object)
	 */
	@SuppressWarnings("unchecked")
	public <T> T getAttachment(String key) {
		if (attachments == null) {
			return null;
		}
		Object res = attachments.get(key);
		return (T) res;
	}

	/**
	 * Retrieves a set of all attachment keys for this HttpMessage
	 */
	public Set<Object> getAttachmentKeys() {
		return attachments != null ? attachments.keySet() : emptySet();
	}

	/**
	 * Sets this message to use the DEFLATE compression algorithm.
	 */
	public void setBodyGzipCompression() {
		this.flags |= USE_GZIP;
	}

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	boolean isRecycled() {
		return (this.flags & RECYCLED) != 0;
	}

	final void recycle() {
		assert !isRecycled();
		flags |= RECYCLED;
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
