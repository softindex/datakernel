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
import java.nio.charset.Charset;

/**
 * A buffer to which char sequences and values can be appended.
 */
public final class BufferAppendable implements Appendable {
	private byte[] buf;
	private int pos;

	// region creators

	/**
	 * Creates a new instance of this class
	 *
	 * @param buf      array of bytes
	 * @param position the index of the next element to be read or written
	 */
	private BufferAppendable(byte[] buf, int position) {
		assert position >= 0 && position <= buf.length;
		this.buf = buf;
		this.pos = position;
	}

	public static BufferAppendable create(byte[] buf, int position) {return new BufferAppendable(buf, position);}
	// endregion

	protected void ensureSize(int size) {
	}

	/**
	 * Returns an  array of bytes for this object
	 */
	public byte[] array() {
		return buf;
	}

	/**
	 * Sets an array of bytes for this object
	 *
	 * @param buf array for setting
	 */
	public void array(byte[] buf) {
		this.buf = buf;
	}

	/**
	 * Sets an array of bytes and position for this object
	 */
	public void set(byte[] buf, int position) {
		assert position >= 0 && position <= buf.length;
		this.buf = buf;
		this.pos = position;
	}

	/**
	 * Returns a position from this buffer
	 */
	public int position() {
		return pos;
	}

	/**
	 * Sets a position for this buffer
	 *
	 * @param position to set
	 */
	public void position(int position) {
		assert position >= 0 && position <= buf.length;
		this.pos = position;
	}

	/**
	 * Returns a number of elements between the current position and the size of buffer.
	 */
	public int remaining() {
		return buf.length - pos;
	}

	private void writeUtfChar(char c) {
		if (c <= 0x07FF) {
			buf[pos] = (byte) (0xC0 | c >> 6 & 0x1F);
			buf[pos + 1] = (byte) (0x80 | c & 0x3F);
			pos += 2;
		} else {
			buf[pos] = (byte) (0xE0 | c >> 12 & 0x0F);
			buf[pos + 1] = (byte) (0x80 | c >> 6 & 0x3F);
			buf[pos + 2] = (byte) (0x80 | c & 0x3F);
			pos += 3;
		}
	}

	/**
	 * Appends a subsequence to this buffer
	 *
	 * @param csq subsequence to append
	 * @return this object
	 */
	@Override
	public Appendable append(CharSequence csq) throws IOException {
		return append(csq, 0, csq.length());
	}

	/**
	 * Appends a subsequence of the specified character sequence to this
	 *
	 * @param csq   the character sequence from which a subsequence will be appended
	 * @param start the index of the first character in the subsequence
	 * @param end   the index of the character following the last character in the subsequence
	 * @return this object
	 */
	@Override
	public Appendable append(CharSequence csq, int start, int end) {
		ensureSize((end - start) * 3);
		try {
			for (int i = start; i < end; i++) {
				char c = csq.charAt(i);
				if (c <= 0x007F) {
					buf[pos++] = (byte) c;
				} else {
					writeUtfChar(c);
				}
			}
			return this;
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new BufferAppendableException();
		}
	}

	/**
	 * Appends the specified character to this
	 *
	 * @param c the character to append
	 * @return this object
	 */
	@Override
	public Appendable append(char c) {
		ensureSize(3);
		try {
			if (c <= 0x007F) {
				buf[pos++] = (byte) c;
			} else {
				writeUtfChar(c);
			}
			return this;
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new BufferAppendableException();
		}
	}

	@Override
	public String toString() {
		return new String(buf, 0, pos, Charset.forName("UTF-8"));
	}
}
