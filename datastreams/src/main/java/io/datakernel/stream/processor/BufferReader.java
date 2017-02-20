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

package io.datakernel.stream.processor;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;

/**
 * Reads characters from a character-input stream, buffers characters
 */
public final class BufferReader extends Reader {
	private byte[] buf;
	private int pos;
	private int limit;

	// region creators
	private BufferReader(byte[] array, int position, int length) {
		set(array, position, length);
	}

	/**
	 * Creates a new instance of this object
	 *
	 * @param array    array for buffering read characters
	 * @param position the index of the next element to be written
	 * @param length   number of bytes which can be written to this array
	 */
	public static BufferReader create(byte[] array, int position, int length) {return new BufferReader(array, position, length);}
	// endregion

	@Override
	public void close() throws IOException {
	}

	public void set(byte[] array, int position, int len) {
		assert position >= 0 && len >= 0;
		this.buf = array;
		this.pos = position;
		this.limit = position + len;
	}

	/**
	 * Returns a buffer for this object
	 */
	public byte[] array() {
		return buf;
	}

	/**
	 * Returns a position for its buffer
	 */
	public int position() {
		return pos;
	}

	/**
	 * Sets a position for its buffer
	 *
	 * @param position position to set
	 */
	public void position(int position) {
		assert pos >= 0;
		pos = position;
	}

	/**
	 * Returns elements between the current position and the size of buffer.
	 */
	private int remaining() {
		return buf.length - pos;
	}

	/**
	 * Reads a single character
	 *
	 * @return the character read, as an integer
	 * @throws IOException if an I/O error occurs
	 */
	@Override
	public int read() throws IOException {
		if (pos >= limit)
			return -1;

		try {
			int c = buf[pos] & 0xff;
			if (c < 0x80) {
				pos++;
			} else if (c < 0xE0) {
				c = (char) ((c & 0x1F) << 6 | buf[pos + 1] & 0x3F);
				pos += 2;
			} else {
				c = (char) ((c & 0x0F) << 12 | (buf[pos + 1] & 0x3F) << 6 | (buf[pos + 2] & 0x3F));
				pos += 3;
			}
			if (pos > limit)
				throw new IOException();
			return c;
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Reads characters into a portion of an array
	 *
	 * @param cbuf destination buffer
	 * @param off  offset at which to start storing characters
	 * @param len  maximum number of characters to read
	 * @return the number of characters read, or -1 if the end of the stream has been reached
	 * @throws IOException if an I/O error occurs
	 */
	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		if (pos > limit)
			return -1;

		try {
			int i;
			int to = off + len;
			for (i = off; i < to && pos < limit; i++) {
				int c = buf[pos] & 0xff;
				if (c < 0x80) {
					cbuf[i] = (char) c;
					pos++;
				} else if (c < 0xE0) {
					cbuf[i] = (char) ((c & 0x1F) << 6 | buf[pos + 1] & 0x3F);
					pos += 2;
				} else {
					cbuf[i] = (char) ((c & 0x0F) << 12 | (buf[pos + 1] & 0x3F) << 6 | (buf[pos + 2] & 0x3F));
					pos += 3;
				}
			}
			if (pos > limit) {
				throw new IOException();
			}
			if (pos == limit) {
				pos = Integer.MAX_VALUE;
			}
			return i - off;
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new IOException(e);
		}
	}

	@Override
	public String toString() {
		return new String(buf, pos, limit - pos, Charset.forName("UTF-8"));
	}
}
