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
import java.util.LinkedHashMap;
import java.util.Map;

import static io.datakernel.util.ByteBufStrings.*;

/**
 * Represents any HTTP message. Its internal byte buffers will be automatically recycled in HTTP client or HTTP server.
 */
public abstract class HttpMessage {
	protected boolean recycled;

	protected final LinkedHashMap<HttpHeader, HttpHeaderValue> headers = new LinkedHashMap<>();
	private ArrayList<ByteBuf> headerBufs;
	protected ByteBuf body;

	protected HttpMessage() {
	}

	/**
	 * Returns headers from this HttpMessage
	 */
	public LinkedHashMap<HttpHeader, HttpHeaderValue> getHeaders() {
		assert !recycled;
		return headers;
	}

	/**
	 * Sets all headers from map from argument to this HttpMessage
	 *
	 * @param headers headers for adding
	 */
	protected void setHeaders(Map<HttpHeader, HttpHeaderValue> headers) {
		assert !recycled;
		this.headers.putAll(headers);
	}

	/**
	 * Adds all headers from map from argument to this HttpMessage
	 *
	 * @param headers headers for adding
	 */

	protected void addHeaders(Map<HttpHeader, HttpHeaderValue> headers) {
		assert !recycled;
		for (Map.Entry<HttpHeader, HttpHeaderValue> entry : headers.entrySet()) {
			HttpHeader header = entry.getKey();
			HttpHeaderValue value = entry.getValue();
			HttpHeaderValue existValue = this.headers.get(header);
			if (existValue != null) {
				value.next(existValue);
			}
			this.headers.put(header, value);
		}
	}

	/**
	 * Sets the header with value to this HttpMessage
	 *
	 * @param header header for adding
	 * @param value  value of this header
	 */
	protected void setHeader(HttpHeader header, HttpHeaderValue value) {
		assert !recycled;
		this.headers.put(header, value);
	}

	/**
	 * Adds the header with value to this HttpMessage
	 *
	 * @param header header for adding
	 * @param value  value of this header
	 */
	protected void addHeader(HttpHeader header, HttpHeaderValue value) {
		assert !recycled;
		HttpHeaderValue existValue = headers.get(header);
		if (existValue != null) {
			value.next(existValue);
		}
		setHeader(header, value);
	}

	/**
	 * Sets the header with value as ByteBuf to this HttpMessage
	 *
	 * @param header header for adding
	 * @param value  value of this header
	 */
	protected void setHeader(HttpHeader header, ByteBuf value) {
		assert !recycled;
		setHeader(header, HttpHeader.valueAsBytes(value.array(), value.position(), value.remaining()));
		if (value.isRecycleNeeded()) {
			if (headerBufs == null) {
				headerBufs = new ArrayList<>(4);
				headerBufs.add(value);
			}
		}
	}

	/**
	 * Adds the header with value as ByteBuf to this HttpMessage
	 *
	 * @param header header for adding
	 * @param value  value of this header
	 */
	protected void addHeader(HttpHeader header, ByteBuf value) {
		assert !recycled;
		addHeader(header, HttpHeader.valueAsBytes(value.array(), value.position(), value.remaining()));
		if (value.isRecycleNeeded()) {
			if (headerBufs == null) {
				headerBufs = new ArrayList<>(4);
				headerBufs.add(value);
			}
		}
	}

	/**
	 * Sets the header with value as array of bytes to this HttpMessage
	 *
	 * @param header header for adding
	 * @param value  value of this header
	 */
	protected void setHeader(HttpHeader header, byte[] value) {
		assert !recycled;
		setHeader(header, HttpHeader.valueAsBytes(value, 0, value.length));
	}

	/**
	 * Adds the header with value as array of bytes to this HttpMessage
	 *
	 * @param header header for adding
	 * @param value  value of this header
	 */
	protected void addHeader(HttpHeader header, byte[] value) {
		assert !recycled;
		addHeader(header, HttpHeader.valueAsBytes(value, 0, value.length));
	}

	/**
	 * Sets the header with value as string to this HttpMessage
	 *
	 * @param header header for adding
	 * @param string value of this header
	 */
	protected void setHeader(HttpHeader header, String string) {
		assert !recycled;
		setHeader(header, HttpHeader.valueOfString(string));
	}

	/**
	 * Adds the header with value as string to this HttpMessage
	 *
	 * @param header header for adding
	 * @param string value of this header
	 */
	protected void addHeader(HttpHeader header, String string) {
		assert !recycled;
		addHeader(header, HttpHeader.valueOfString(string));
	}

	/**
	 * Returns the last value of header from this message
	 *
	 * @param header header for getting
	 */
	public HttpHeaderValue getHeaderValue(HttpHeader header) {
		assert !recycled;
		return headers.get(header);
	}

	/**
	 * Returns the last value of header as string from this message
	 *
	 * @param header header for getting
	 */
	public String getHeaderString(HttpHeader header) {
		assert !recycled;
		HttpHeaderValue value = getHeaderValue(header);
		if (value == null) {
			return null;
		}
		return value.toString();
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
		for (Map.Entry<HttpHeader, HttpHeaderValue> entry : this.headers.entrySet()) {
			HttpHeader header = entry.getKey();
			for (HttpHeaderValue value = entry.getValue(); value != null; value = value.next()) {
				buf.set(0, CR);
				buf.set(1, LF);
				buf.advance(2);
				header.writeTo(buf);
				buf.set(0, (byte) ':');
				buf.set(1, SP);
				buf.advance(2);
				value.writeTo(buf);
			}
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
		for (Map.Entry<HttpHeader, HttpHeaderValue> entry : this.headers.entrySet()) {
			HttpHeader header = entry.getKey();
			for (HttpHeaderValue value = entry.getValue(); value != null; value = value.next())
				size += 2 + header.size() + 2 + value.estimateSize(); // CR,LF,header,": ",value
		}
		size += 4; // CR,LF,CR,LF
		if (body != null)
			size += body.remaining();
		return size;
	}

}
