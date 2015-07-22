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

package io.datakernel.bytebuf;

import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;

/**
 * Represent a buffer which can be recycled from itself.Byte buffers can be created either by allocation,
 * which allocates space for the buffer's content,or by wrapping an existing byte array into a buffer.
 * After using this ByteBuf, call its recycle, but after it you can not work with its instance.
 * You may don't recycle it, after it this ByteBuf can be removing with the GC
 */
public class ByteBuf {
	protected int refs;

	private static final byte[] EMPTY_ARRAY = new byte[0];
	private static final ByteBuf EMPTY_BUF = new ByteBuf(EMPTY_ARRAY, 0, 0);

	protected final byte[] array;
	protected int position;
	protected int limit;

	private ByteBuf(byte[] array, int position, int limit) {
		assert position >= 0 && position <= limit && limit <= array.length;
		this.array = array;
		this.position = position;
		this.limit = limit;
	}

	/**
	 * Returns an empty buffer, which position and limit will are zero.
	 */
	public static ByteBuf empty() {
		assert EMPTY_BUF.position == 0;
		assert EMPTY_BUF.limit == 0;
		return EMPTY_BUF;
	}

	/**
	 * Allocates a new  byte ByteBuf. The new buffer's position will be zero, its limit will be its
	 * size.
	 *
	 * @param size new buffers capacity in bytes
	 * @return the new ByteBuf
	 */
	public static ByteBuf allocate(int size) {
		return new ByteBuf(new byte[size], 0, size);
	}

	/**
	 * Wraps a byte array into a ByteBuf.
	 * The new buffer will be backed by the given byte array.The new buffer's capacity and limit will be
	 * array.length, its position will be zero
	 *
	 * @param array the array that will back this buffer
	 * @return the new ByteBuf
	 */
	public static ByteBuf wrap(byte[] array) {
		return new ByteBuf(array, 0, array.length);
	}

	/**
	 * Wraps a byte array into a ByteBuf.
	 * The new buffer will be backed by the given byte array. The new buffer's capacity will be array.length,
	 * its position will be offset, its limit will be offset + length.
	 *
	 * @param array  the array that will back this buffer
	 * @param offset offset of the subarray to be used; must be non-negative and no larger than array.length.
	 *               The new buffer's position will be set to this value.
	 * @param length length of the subarray to be used; must be non-negative and no larger than array.length
	 *               - offset. The new buffer's limit will be set to offset + length.
	 * @return the new ByteBuf
	 */
	public static ByteBuf wrap(byte[] array, int offset, int length) {
		return new ByteBuf(array, offset, offset + length);
	}

	private final static class ByteBufSlice extends ByteBuf {
		private final ByteBuf root;

		private ByteBufSlice(ByteBuf root, byte[] array, int position, int limit) {
			super(array, position, limit);
			this.root = root;
		}

		@Override
		public void recycle() {
			root.recycle();
		}

		@Override
		public ByteBuf slice(int offset, int length) {
			return root.slice(offset, length);
		}

		@Override
		boolean isRecycled() {
			return root.isRecycled();
		}

		@Override
		public boolean isRecycleNeeded() {
			return root.isRecycleNeeded();
		}
	}

	public ByteBuf slice(int offset, int length) {
		assert !isRecycled();
		if (!isRecycleNeeded()) {
			return ByteBuf.wrap(array, offset, length);
		}
		refs++;
		return new ByteBufSlice(this, array, offset, offset + length);
	}

	/**
	 * Wraps a this ByteBuf into a ByteBuffer.
	 * The new ByteBuffer will be backed by the given ByteBuf. The new buffer's capacity will be capacity of
	 * ByteBuf, its position will be position of ByteBuf, its limit will be limit of this ByteBuf.
	 *
	 * @return the new ByteBuf
	 */
	public ByteBuffer toByteBuffer() {
		ByteBuffer byteBuffer = ByteBuffer.wrap(array);
		byteBuffer.limit(limit);
		byteBuffer.position(position);
		return byteBuffer;
	}

	/**
	 * Sets position and limit of this ByteBuf to be same as appropriate ByteBuffer values
	 *
	 * @param byteBuffer from it takes new values for position and limit
	 */
	public void setByteBuffer(ByteBuffer byteBuffer) {
		assert !isRecycled();
		assert this.array == byteBuffer.array();
		assert byteBuffer.arrayOffset() == 0;
		position(byteBuffer.position());
		limit(byteBuffer.limit());
	}

	/**
	 * Tells whether or not this byte buffer is recycled.
	 */
	@VisibleForTesting
	boolean isRecycled() {
		return refs == -1;
	}

	/**
	 * Resets this ByteBuf, puts it back to byteBufPool
	 */
	public void recycle() {
		assert !isRecycled();
		if (refs > 0 && --refs == 0) {
			assert --refs == -1;
			ByteBufPool.recycle(this);
		}
	}

	/**
	 * Tells whether or not this byte buffer needs recycling.
	 */
	public boolean isRecycleNeeded() {
		return refs > 0;
	}

	/**
	 * Flips this buffer. The limit is set to the current position and then the position is set to zero.
	 */
	public void flip() {
		assert !isRecycled();
		limit = position;
		position = 0;
	}

	/**
	 * Returns the number of elements between the current position and the limit.
	 *
	 * @return the number of elements remaining in this buffer
	 */
	public int remaining() {
		assert !isRecycled();
		return limit - position;
	}

	/**
	 * Tells whether there are any elements between the current position and the limit.
	 *
	 * @return true if, and only if, there is at least one element remaining in this buffer
	 */
	public boolean hasRemaining() {
		assert !isRecycled();
		return position != limit;
	}

	/**
	 * Reads the byte at this buffer's current position, and then increments the position.
	 *
	 * @return the  byte at the buffer's current position
	 */
	public byte get() {
		assert !isRecycled();
		assert position < limit;
		return array[position++];
	}

	/**
	 * Reads the byte after at this buffer's current position on i bytes , and does not change
	 * the position.
	 *
	 * @return the byte which position is shifted by i from buffer's current position
	 */
	public byte peek(int i) {
		assert !isRecycled();
		assert (position + i) < limit;
		return array[position + i];
	}

	/**
	 * Reads the byte at this buffer's current position, and does not increment the position.
	 *
	 * @return the  byte at the buffer's current position
	 */
	public byte peek() {
		assert !isRecycled();
		return array[position];
	}

	/**
	 * Returns the byte array that backs this buffer.
	 * Modifications to this buffer's content will cause the returned array's content to be modified
	 *
	 * @return the array that backs this buffer
	 */
	public byte[] array() {
		assert !isRecycled();
		return array;
	}

	/**
	 * Returns byte at absolute position of internal array
	 *
	 * @param index absolute index of byte, which will be returned
	 */
	public byte at(int index) {
		assert !isRecycled();
		assert index <= array.length;
		return array[index];
	}

	/**
	 * Sets this buffer's limit.
	 *
	 * @param limit the new limit
	 */
	public void limit(int limit) {
		assert !isRecycled();
		assert limit >= this.position && limit <= array.length;
		this.limit = limit;
	}

	/**
	 * Returns this buffer's capacity.
	 */
	@VisibleForTesting
	int capacity() {
		return array.length;
	}

	/**
	 * Returns this buffer's position.
	 */
	public int position() {
		assert !isRecycled();
		return position;
	}

	/**
	 * Sets this buffer's position
	 *
	 * @param position position to be set
	 */
	public void position(int position) {
		assert !isRecycled();
		assert position >= 0 && position <= this.limit;
		this.position = position;
	}

	/**
	 * Increases by size position for this buffer
	 *
	 * @param size number for increasing
	 */
	public void advance(int size) {
		assert !isRecycled();
		assert size >= 0 && (this.position + size) <= this.limit;
		this.position += size;
	}

	/**
	 * Writes the given byte into this buffer at the current position, and then increments the position.
	 *
	 * @param b the byte to be written
	 */
	public void put(byte b) {
		assert !isRecycled();
		assert position < limit;
		array[position++] = b;
	}

	/**
	 * Transfers part or the entire content of the given source byte array into this buffer.
	 *
	 * @param array  the array for transferring
	 * @param offset starting position in the source array
	 * @param size   number of bytes for transferring
	 */
	public void put(byte[] array, int offset, int size) {
		assert !isRecycled();
		assert size >= 0 && (offset + size) <= array.length;
		assert (this.position + size) <= this.limit;
		System.arraycopy(array, offset, this.array, this.position, size);
		this.position += size;
	}

	/**
	 * Transfers entire content of source byte array into this buffer.
	 *
	 * @param array the array for transferring
	 */
	public void put(byte[] array) {
		assert !isRecycled();
		put(array, 0, array.length);
	}

	/**
	 * Transfers all remaining bytes of the given ByteBuf into this buffer.
	 *
	 * @param buf the ByteBuf for transferring
	 */
	public void put(ByteBuf buf) {
		assert !isRecycled();
		assert (this.position + buf.remaining()) <= this.limit;
		System.arraycopy(buf.array, buf.position, this.array, this.position, buf.remaining());
		this.position += buf.remaining();
	}

	/**
	 * Sets the byte with index = (position + offset) to b
	 *
	 * @param offset position for setting byte
	 * @param b      the byte for setting
	 */
	public void set(int offset, byte b) {
		assert !isRecycled();
		assert (this.position + offset) < this.limit;
		array[position + offset] = b;
	}

	/**
	 * Adds size elements after position to array. Sets the position of this buffer after size elements
	 *
	 * @param array  the array for draining
	 * @param offset starting position for adding elements to array
	 * @param size   number of elements to be drained
	 */
	public void drainTo(byte[] array, int offset, int size) {
		assert !isRecycled();
		assert size >= 0 && (offset + size) <= array.length;
		assert (this.position + size) <= this.limit;
		System.arraycopy(this.array, this.position, array, offset, size);
		this.position += size;
	}

	/**
	 * Adds size elements after position to buffer from argument. Sets the position of this buffer and buf
	 * after size elements.
	 *
	 * @param buf  the buffer for draining
	 * @param size number of elements to be drained
	 */
	public void drainTo(ByteBuf buf, int size) {
		assert !isRecycled();
		assert size >= 0 && (buf.position + size) <= buf.limit;
		assert (this.position + size) <= this.limit;
		System.arraycopy(this.array, this.position, buf.array, buf.position, size);
		this.position += size;
		buf.position += size;
	}

	/**
	 * Returns limit of this buffer
	 */
	public int limit() {
		assert !isRecycled();
		return limit;
	}

	public boolean equalsTo(byte[] array, int offset, int size) {
		assert !isRecycled();
		if (remaining() != size)
			return false;
		assert (this.position + size) <= this.limit;
		for (int i = 0; i != size; i++) {
			if (this.array[i + position] != array[i + offset])
				return false;
		}
		return true;
	}

	public boolean equalsTo(ByteBuf buf) {
		assert !isRecycled();
		return equalsTo(buf.array, buf.position, buf.remaining());
	}

	public boolean equalsTo(byte[] array) {
		assert !isRecycled();
		return equalsTo(array, 0, array.length);
	}

	@Override
	public String toString() {
		char[] chars = new char[remaining() < 256 ? remaining() : 256];
		for (int i = 0; i < chars.length; i++) {
			byte b = array[position + i];
			chars[i] = (b >= ' ') ? (char) b : (char) 65533;
		}
		return new String(chars);
	}
}
