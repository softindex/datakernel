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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.util.ByteBufStrings;

import java.nio.charset.Charset;
import java.util.*;

import static io.datakernel.http.HttpHeader.CONTENT_TYPE;
import static io.datakernel.util.ByteBufStrings.*;

/**
 * Represents any HTTP message. Its internal byte buffers will be automatically recycled in HTTP client or HTTP server.
 */
public abstract class HttpMessage {
	protected boolean recycled;

	private final ArrayList<HttpHeader.Value> headers = new ArrayList<>();
	private ArrayList<ByteBuf> headerBufs;
	protected ByteBuf body;

	protected HttpMessage() {
	}

	public List<HttpHeader.Value> getHeaders() {
		assert !recycled;
		return headers;
	}

	// common header setters
	protected void setHeaders(Collection<HttpHeader.Value> headers) {
		assert !recycled;
		assert Collections.disjoint(this.headers, headers) : "Duplicate headers: " + this.headers + " : " + headers;
		this.headers.addAll(headers);
	}

	protected void addHeaders(Collection<HttpHeader.Value> headers) {
		assert !recycled;
		this.headers.addAll(headers);
	}

	/**
	 * Sets the header with value to this HttpMessage.
	 * Checks whether the header was already applied to the message.
	 *
	 * @param value value of this header
	 */
	protected void setHeader(HttpHeader.Value value) {
		assert !recycled;
		assert getHeader(value.getKey()) == null : "Duplicate header: " + value.getKey();
		headers.add(value);
	}

	/**
	 * Adds the header with value to this HttpMessage
	 * Does not check whether the header was already applied to the message.
	 *
	 * @param value value of this header
	 */
	protected void addHeader(HttpHeader.Value value) {
		assert !recycled;
		headers.add(value);
	}

	protected void setHeader(HttpHeader header, ByteBuf value) {
		assert !recycled;
		setHeader(HttpHeader.asBytes(header, value.array(), value.position(), value.remaining()));
		if (value.isRecycleNeeded()) {
			if (headerBufs == null) {
				headerBufs = new ArrayList<>(4);
			}
			headerBufs.add(value);
		}
	}

	protected void addHeader(HttpHeader header, ByteBuf value) {
		assert !recycled;
		addHeader(HttpHeader.asBytes(header, value.array(), value.position(), value.remaining()));
		if (value.isRecycleNeeded()) {
			if (headerBufs == null) {
				headerBufs = new ArrayList<>(4);
			}
			headerBufs.add(value);
		}
	}

	protected void setHeader(HttpHeader header, byte[] value) {
		assert !recycled;
		setHeader(HttpHeader.asBytes(header, value, 0, value.length));
	}

	protected void addHeader(HttpHeader header, byte[] value) {
		assert !recycled;
		addHeader(HttpHeader.asBytes(header, value, 0, value.length));
	}

	protected void setHeader(HttpHeader header, String string) {
		assert !recycled;
		setHeader(HttpHeader.ofString(header, string));
	}

	protected void addHeader(HttpHeader header, String string) {
		assert !recycled;
		addHeader(HttpHeader.ofString(header, string));
	}

	// special header setters
	protected void addCookieHeader(HttpHeader header, HttpCookie cookie) {
		assert !recycled;
		addHeader(HttpHeader.ofCookie(header, cookie));
	}

	protected void addCookieHeader(HttpHeader header, List<HttpCookie> cookies) {
		assert !recycled;
		addHeader(HttpHeader.ofCookies(header, cookies));
	}

	protected void addContentTypeHeader(HttpHeader header, List<ContentType> type) {
		assert !recycled;
		addHeader(HttpHeader.ofContentType(header, type));
	}

	protected void addContentTypeHeader(HttpHeader header, ContentType type) {
		assert !recycled;
		addHeader(HttpHeader.ofContentType(header, Collections.singletonList(type)));
	}

	protected void addCharsetHeader(HttpHeader header, Charset charset) {
		assert !recycled;
		addHeader(HttpHeader.ofCharsets(header, Collections.singletonList(new HttpUtils.Pair<>(charset))));
	}

	protected void addCharsetRawHeader(HttpHeader header, List<Charset> charsets) {
		assert !recycled;
		List<HttpUtils.Pair<Charset>> ch = new ArrayList<>();
		for (Charset charset : charsets) {
			ch.add(new HttpUtils.Pair<>(charset));
		}
		addHeader(HttpHeader.ofCharsets(header, ch));
	}

	protected void addCharsetPairHeader(HttpHeader header, List<HttpUtils.Pair<Charset>> charsets) {
		assert !recycled;
		addHeader(HttpHeader.ofCharsets(header, charsets));
	}

	protected void setHeader(HttpHeader header, int value) {
		assert !recycled;
		setHeader(HttpHeader.ofDecimal(header, value));
	}

	protected void addHeader(HttpHeader header, int value) {
		assert !recycled;
		addHeader(HttpHeader.ofDecimal(header, value));
	}

	protected void setHeader(HttpHeader header, Date value) {
		assert !recycled;
		setHeader(HttpHeader.ofDate(header, value));
	}

	protected void setBody(ByteBuf body) {
		assert !recycled;
		if (this.body != null)
			this.body.recycle();
		this.body = body;
	}

	public ByteBuf getBody() {
		assert !recycled;
		return body;
	}

	// specs
	public int getContentLength() {
		assert !recycled;
		String value = getHeaderString(HttpHeader.CONTENT_LENGTH);
		if (value == null || value.equals("")) {
			return -1;
		}
		return ByteBufStrings.decodeDecimal(value.getBytes(Charset.forName("ISO-8859-1")), 0, value.length());
	}

	public List<ContentType> getContentType() {
		assert !recycled;
		List<ContentType> cts = new ArrayList<>();
		List<String> headers = getHeaderStrings(CONTENT_TYPE);
		for (String header : headers) {
			ContentType.parse(header, cts);
		}
		return cts;
	}

	public Date getDate() {
		assert !recycled;
		String value = getHeaderString(HttpHeader.DATE);
		if (value != null && !value.equals("")) {
			long timestamp = HttpDate.parse(ByteBufStrings.encodeAscii(value), 0);
			return new Date(timestamp);
		}
		return null;
	}

	/**
	 * Removes the body of this message and returns it. After its method, owner of
	 * body of this HttpMessage is changed, and it will not be automatically recycled in HTTP client or HTTP server.
	 *
	 * @return the body
	 */
	public ByteBuf detachBody() {
		ByteBuf buf = body;
		body = null;
		return buf;
	}

	/**
	 * Recycles body and header. You should do it before reusing.
	 */
	protected void recycleBufs() {
		assert !recycled;
		if (body != null) {
			body.recycle();
			body = null;
		}
		if (headerBufs != null) {
			for (ByteBuf headerBuf : headerBufs) {
				headerBuf.recycle();
			}
			headerBufs = null;
		}
		recycled = true;
	}

	/**
	 * Sets headers for this message from ByteBuf
	 *
	 * @param buf the new headers
	 */
	protected void writeHeaders(ByteBuf buf) {
		assert !recycled;
		for (HttpHeader.Value entry : this.headers) {
			HttpHeader header = entry.getKey();

			buf.set(0, CR);
			buf.set(1, LF);
			buf.advance(2);
			header.writeTo(buf);
			buf.set(0, (byte) ':');
			buf.set(1, SP);
			buf.advance(2);
			entry.writeTo(buf);
		}

		buf.set(0, CR);
		buf.set(1, LF);
		buf.set(2, CR);
		buf.set(3, LF);
		buf.advance(4);
	}

	protected void writeBody(ByteBuf buf) {
		assert !recycled;
		if (body != null) {
			buf.put(body);
		}
	}

	protected int estimateSize(int firstLineSize) {
		assert !recycled;
		int size = firstLineSize;
		for (HttpHeader.Value entry : this.headers) {
			HttpHeader header = entry.getKey();
			size += 2 + header.size() + 2 + entry.estimateSize(); // CR,LF,header,": ",value
		}
		size += 4; // CR,LF,CR,LF
		if (body != null)
			size += body.remaining();
		return size;
	}

	public final HttpHeader.Value getHeader(HttpHeader header) {
		if (header instanceof HttpHeader.HttpCustomHeader) {
			HttpHeader.HttpCustomHeader httpCustomHeader = (HttpHeader.HttpCustomHeader) header;
			for (HttpHeader.Value headerValue : headers) {
				if (httpCustomHeader.equals(headerValue.getKey()))
					return headerValue;
			}
		} else {
			for (HttpHeader.Value headerValue : headers) {
				if (header == headerValue.getKey())
					return headerValue;
			}
		}
		return null;
	}

	public final String getHeaderString(HttpHeader header) {
		HttpHeader.Value result = getHeader(header);
		return result == null ? null : result.toString();
	}

	public final List<HttpHeader.Value> getHeaders(HttpHeader header) {
		List<HttpHeader.Value> result = new ArrayList<>();
		if (header instanceof HttpHeader.HttpCustomHeader) {
			HttpHeader.HttpCustomHeader httpCustomHeader = (HttpHeader.HttpCustomHeader) header;
			for (HttpHeader.Value headerValue : headers) {
				if (httpCustomHeader.equals(headerValue.getKey()))
					result.add(headerValue);
			}
		} else {
			for (HttpHeader.Value headerValue : headers) {
				if (header == headerValue.getKey())
					result.add(headerValue);
			}
		}
		return result;
	}

	public final List<String> getHeaderStrings(HttpHeader header) {
		List<String> result = new ArrayList<>();
		if (header instanceof HttpHeader.HttpCustomHeader) {
			HttpHeader.HttpCustomHeader httpCustomHeader = (HttpHeader.HttpCustomHeader) header;
			for (HttpHeader.Value headerValue : headers) {
				if (httpCustomHeader.equals(headerValue.getKey()))
					result.add(headerValue.toString());
			}
		} else {
			for (HttpHeader.Value headerValue : headers) {
				if (header == headerValue.getKey())
					result.add(headerValue.toString());
			}
		}
		return result;
	}
}