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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.util.ByteBufStrings.*;

/**
 * Represents any HTTP message. Its internal byte buffers will be automatically recycled in HTTP client or HTTP server.
 */
public abstract class HttpMessage {
	protected boolean recycled;

	private final ArrayList<HttpHeaders.Value> headers = new ArrayList<>();
	private ArrayList<ByteBuf> headerBufs;
	protected ByteBuf body;

	protected HttpMessage() {
	}

	public List<HttpHeaders.Value> getHeaders() {
		assert !recycled;
		return headers;
	}

	/**
	 * Sets the header with value to this HttpMessage.
	 * Checks whether the header was already applied to the message.
	 *
	 * @param value value of this header
	 */
	protected void setHeader(HttpHeaders.Value value) {
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
	protected void addHeader(HttpHeaders.Value value) {
		assert !recycled;
		headers.add(value);
	}

	protected void setHeader(HttpHeader header, ByteBuf value) {
		assert !recycled;
		setHeader(HttpHeaders.asBytes(header, value.array(), value.position(), value.remaining()));
		if (value.isRecycleNeeded()) {
			if (headerBufs == null) {
				headerBufs = new ArrayList<>(4);
			}
			headerBufs.add(value);
		}
	}

	protected void addHeader(HttpHeader header, ByteBuf value) {
		assert !recycled;
		addHeader(HttpHeaders.asBytes(header, value.array(), value.position(), value.remaining()));
		if (value.isRecycleNeeded()) {
			if (headerBufs == null) {
				headerBufs = new ArrayList<>(4);
			}
			headerBufs.add(value);
		}
	}

	protected void setHeader(HttpHeader header, byte[] value) {
		assert !recycled;
		setHeader(HttpHeaders.asBytes(header, value, 0, value.length));
	}

	protected void addHeader(HttpHeader header, byte[] value) {
		assert !recycled;
		addHeader(HttpHeaders.asBytes(header, value, 0, value.length));
	}

	protected void setHeader(HttpHeader header, String string) {
		assert !recycled;
		setHeader(HttpHeaders.ofString(header, string));
	}

	protected void addHeader(HttpHeader header, String string) {
		assert !recycled;
		addHeader(HttpHeaders.ofString(header, string));
	}

	protected void setBody(ByteBuf body) {
		assert !recycled;
		if (this.body != null)
			this.body.recycle();
		this.body = body;
	}

	// getters
	public int getContentLength() {
		assert !recycled;
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeader(CONTENT_LENGTH);
		if (header != null)
			return ByteBufStrings.decodeDecimal(header.array, header.offset, header.size);
		return 0;
	}

	public ContentType getContentType() {
		assert !recycled;
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeader(CONTENT_TYPE);
		if (header != null)
			return ContentType.parse(header.array, header.offset, header.size);
		return null;
	}

	public Date getDate() {
		assert !recycled;
		HttpHeaders.ValueOfBytes header = (HttpHeaders.ValueOfBytes) getHeader(DATE);
		if (header != null) {
			long date = HttpDate.parse(header.array, header.offset);
			return new Date(date);
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

	public ByteBuf getBody() {
		assert !recycled;
		return body;
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
		for (HttpHeaders.Value entry : this.headers) {
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
		for (HttpHeaders.Value entry : this.headers) {
			HttpHeader header = entry.getKey();
			size += 2 + header.size() + 2 + entry.estimateSize(); // CR,LF,header,": ",value
		}
		size += 4; // CR,LF,CR,LF
		if (body != null)
			size += body.remaining();
		return size;
	}

	public final HttpHeaders.Value getHeader(HttpHeader header) {
		for (HttpHeaders.Value headerValue : headers) {
			if (header.equals(headerValue.getKey()))
				return headerValue;
		}
		return null;
	}

	public final String getHeaderString(HttpHeader header) {
		HttpHeaders.Value result = getHeader(header);
		return result == null ? null : result.toString();
	}

	public final List<HttpHeaders.Value> getHeaders(HttpHeader header) {
		List<HttpHeaders.Value> result = new ArrayList<>();
		for (HttpHeaders.Value headerValue : headers) {
			if (header.equals(headerValue.getKey()))
				result.add(headerValue);
		}
		return result;
	}

	public final List<String> getHeaderStrings(HttpHeader header) {
		List<String> result = new ArrayList<>();

		for (HttpHeaders.Value headerValue : headers) {
			if (header.equals(headerValue.getKey()))
				result.add(headerValue.toString());
		}

		return result;
	}
}