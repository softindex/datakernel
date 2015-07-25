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

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.ByteBufStrings.*;

/**
 * Represents any HTTP message. Its internal byte buffers will be automatically recycled in HTTP client or HTTP server.
 */
public abstract class HttpMessage {
	protected boolean recycled;

	private final ArrayList<HttpHeaderValue> headers = new ArrayList<>();
	private ArrayList<ByteBuf> headerBufs;
	protected ByteBuf body;

	protected HttpMessage() {
	}

	/**
	 * Returns headers from this HttpMessage
	 */
	public List<HttpHeaderValue> getHeaders() {
		assert !recycled;
		return headers;
	}

	/**
	 * Adds all headers from map from argument to this HttpMessage
	 *
	 * @param headers headers for adding
	 */

	protected void addHeaders(List<HttpHeaderValue> headers) {
		assert !recycled;
		this.headers.addAll(headers);
	}

	/**
	 * Adds the header with value to this HttpMessage
	 *
	 * @param value value of this header
	 */
	public void addHeader(HttpHeaderValue value) {
		assert !recycled;
		headers.add(value);
	}

	/**
	 * Adds the header with value as ByteBuf to this HttpMessage
	 *
	 * @param header header for adding
	 * @param value  value of this header
	 */
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

	/**
	 * Adds the header with value as array of bytes to this HttpMessage
	 *
	 * @param header header for adding
	 * @param value  value of this header
	 */
	protected void addHeader(HttpHeader header, byte[] value) {
		assert !recycled;
		addHeader(HttpHeader.asBytes(header, value, 0, value.length));
	}

	/**
	 * Adds the header with value as string to this HttpMessage
	 *
	 * @param header header for adding
	 * @param string value of this header
	 */
	protected void addHeader(HttpHeader header, String string) {
		assert !recycled;
		addHeader(HttpHeader.ofString(header, string));
	}

	/**
	 * Returns the body of this message
	 */
	public ByteBuf getBody() {
		assert !recycled;
		return body;
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
	 * Sets the body for this message
	 *
	 * @param body the new body
	 */
	protected void setBody(ByteBuf body) {
		assert !recycled;
		if (this.body != null)
			this.body.recycle();
		this.body = body;
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
		for (HttpHeaderValue entry : this.headers) {
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
		for (HttpHeaderValue entry : this.headers) {
			HttpHeader header = entry.getKey();
			size += 2 + header.size() + 2 + entry.estimateSize(); // CR,LF,header,": ",value
		}
		size += 4; // CR,LF,CR,LF
		if (body != null)
			size += body.remaining();
		return size;
	}

	public final HttpHeaderValue getHeader(HttpHeader header) {
		if (header instanceof HttpHeader.HttpCustomHeader) {
			HttpHeader.HttpCustomHeader httpCustomHeader = (HttpHeader.HttpCustomHeader) header;
			for (HttpHeaderValue headerValue : headers) {
				if (httpCustomHeader.equals(headerValue.getKey()))
					return headerValue;
			}
		} else {
			for (HttpHeaderValue headerValue : headers) {
				if (header == headerValue.getKey())
					return headerValue;
			}
		}
		return null;
	}

	public final String getHeaderString(HttpHeader header) {
		HttpHeaderValue result = getHeader(header);
		return result == null ? null : result.toString();
	}

	public final List<HttpHeaderValue> getHeaders(HttpHeader header) {
		List<HttpHeaderValue> result = new ArrayList<>();
		if (header instanceof HttpHeader.HttpCustomHeader) {
			HttpHeader.HttpCustomHeader httpCustomHeader = (HttpHeader.HttpCustomHeader) header;
			for (HttpHeaderValue headerValue : headers) {
				if (httpCustomHeader.equals(headerValue.getKey()))
					result.add(headerValue);
			}
		} else {
			for (HttpHeaderValue headerValue : headers) {
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
			for (HttpHeaderValue headerValue : headers) {
				if (httpCustomHeader.equals(headerValue.getKey()))
					result.add(headerValue.toString());
			}
		} else {
			for (HttpHeaderValue headerValue : headers) {
				if (header == headerValue.getKey())
					result.add(headerValue.toString());
			}
		}
		return result;
	}

}
