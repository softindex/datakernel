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
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.serial.SerialSupplier;

import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpHeaders.DATE;
import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * Represents any HTTP message. Its internal byte buffers will be automatically recycled in HTTP client or HTTP server.
 */
public abstract class HttpMessage {
	protected ArrayList<HttpHeaders.Value> headers = new ArrayList<>();
	private ArrayList<ByteBuf> headerBufs;
	protected SerialSupplier<ByteBuf> bodySupplier;
	protected ByteBuf body;
	protected boolean useGzip;

	protected HttpMessage() {
	}

	public final Map<HttpHeader, String> getHeaders() {
		LinkedHashMap<HttpHeader, String> map = new LinkedHashMap<>(headers.size() * 2);
		for (HttpHeaders.Value headerValue : headers) {
			HttpHeader header = headerValue.getKey();
			String headerString = headerValue.toString();
			if (!map.containsKey(header)) {
				map.put(header, headerString);
			}
		}
		return map;
	}

	public final Map<HttpHeader, List<String>> getAllHeaders() {
		LinkedHashMap<HttpHeader, List<String>> map = new LinkedHashMap<>(headers.size() * 2);
		for (HttpHeaders.Value headerValue : headers) {
			HttpHeader header = headerValue.getKey();
			String headerString = headerValue.toString();
			map.computeIfAbsent(header, k -> new ArrayList<>()).add(headerString);
		}
		return map;
	}

	/**
	 * Sets the header with value to this HttpMessage.
	 * Checks whether the header was already applied to the message.
	 *
	 * @param value value of this header
	 */
	protected void setHeader(HttpHeaders.Value value) {
		assert !isRecycled();
		assert getHeaderValue(value.getKey()) == null : "Duplicate header: " + value.getKey();
		headers.add(value);
	}

	/**
	 * Adds the header with value to this HttpMessage
	 * Does not check whether the header was already applied to the message.
	 *
	 * @param value value of this header
	 */
	protected void addHeader(HttpHeaders.Value value) {
		assert !isRecycled();
		headers.add(value);
	}

	public void addHeader(HttpHeader header, ByteBuf value) {
		assert !isRecycled();
		addHeader(HttpHeaders.asBytes(header, value.array(), value.readPosition(), value.readRemaining()));
		if (value.isRecycleNeeded()) {
			if (headerBufs == null) {
				headerBufs = new ArrayList<>(4);
			}
			headerBufs.add(value);
		}
	}

	public void addHeader(HttpHeader header, byte[] value) {
		assert !isRecycled();
		addHeader(HttpHeaders.asBytes(header, value, 0, value.length));
	}

	public void addHeader(HttpHeader header, String string) {
		assert !isRecycled();
		addHeader(HttpHeaders.ofString(header, string));
	}

	public void setBodyGzipCompression() {
		this.useGzip = true;
	}

	// getters
	public ContentType getContentType() {
		assert !isRecycled();
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeaderValue(CONTENT_TYPE);
		if (header != null) {
			try {
				return ContentType.parse(header.array, header.offset, header.size);
			} catch (ParseException e) {
				return null;
			}
		}
		return null;
	}

	public Date getDate() {
		assert !isRecycled();
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeaderValue(DATE);
		if (header != null) {
			try {
				long date = HttpDate.parse(header.array, header.offset);
				return new Date(date);
			} catch (ParseException e) {
				return null;
			}
		}
		return null;
	}

	public ByteBuf getBody() {
		return checkNotNull(body);
	}

	public ByteBuf detachBody() {
		ByteBuf body = checkNotNull(this.body);
		this.body = null;
		return body;
	}

	public Stage<ByteBuf> getBodyStage() {
		if (body != null) return Stage.of(body);
		if (bodySupplier != null) {
			return bodySupplier.toCollector(ByteBufQueue.collector())
					.whenComplete((buf, e) -> {
						this.body = buf;
						this.bodySupplier = null;
					});
		}
		return Stage.of(ByteBuf.empty());
	}

	protected Stage<? extends HttpMessage> doEnsureBody() {
		if (body != null) return Stage.of(this);
		if (bodySupplier != null) {
			return bodySupplier.toCollector(ByteBufQueue.collector())
					.thenComposeEx((buf, e) -> {
						this.body = buf;
						this.bodySupplier = null;
						return e == null ? Stage.of(this) : Stage.ofException(e);
					});
		}
		return Stage.of(this);
	}

	public SerialSupplier<ByteBuf> getBodyStream() {
		if (body != null) return SerialSupplier.of(body);
		if (bodySupplier != null) return bodySupplier;
		return SerialSupplier.of(ByteBuf.empty());
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
		headers = null;
		if (headerBufs != null) {
			for (ByteBuf headerBuf : headerBufs) {
				headerBuf.recycle();
			}
			headerBufs.clear();
		}
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
		for (HttpHeaders.Value entry : this.headers) {
			HttpHeader header = entry.getKey();

			buf.put(CR);
			buf.put(LF);
			header.writeTo(buf);
			buf.put((byte) ':');
			buf.put(SP);
			entry.writeTo(buf);
		}

		buf.put(CR);
		buf.put(LF);
		buf.put(CR);
		buf.put(LF);
	}

	protected int estimateSize(int firstLineSize) {
		assert !isRecycled();
		int size = firstLineSize;
		for (HttpHeaders.Value entry : this.headers) {
			HttpHeader header = entry.getKey();
			size += 2 + header.size() + 2 + entry.estimateSize(); // CR,LF,header,": ",value
		}
		size += 4; // CR,LF,CR,LF
		return size;
	}

	protected final HttpHeaders.Value getHeaderValue(HttpHeader header) {
		for (HttpHeaders.Value headerValue : headers) {
			if (header.equals(headerValue.getKey()))
				return headerValue;
		}
		return null;
	}

	public final String getHeader(HttpHeader header) {
		HttpHeaders.Value result = getHeaderValue(header);
		return result == null ? null : result.toString();
	}

	protected final List<HttpHeaders.Value> getHeaderValues(HttpHeader header) {
		List<HttpHeaders.Value> result = new ArrayList<>();
		for (HttpHeaders.Value headerValue : headers) {
			if (header.equals(headerValue.getKey()))
				result.add(headerValue);
		}
		return result;
	}

	protected abstract List<HttpCookie> getCookies();

	public Map<String, HttpCookie> getCookiesMap() {
		assert !isRecycled();
		List<HttpCookie> cookies = getCookies();
		LinkedHashMap<String, HttpCookie> map = new LinkedHashMap<>();
		for (HttpCookie cookie : cookies) {
			map.put(cookie.getName(), cookie);
		}
		return map;
	}

	public HttpCookie getCookie(String name) {
		assert !isRecycled();
		List<HttpCookie> cookies = getCookies();
		for (HttpCookie cookie : cookies) {
			if (name.equals(cookie.getName()))
				return cookie;
		}
		return null;
	}

	protected abstract int estimateSize();

	protected abstract void writeTo(ByteBuf buf);
}
