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

import com.google.common.annotations.VisibleForTesting;
import io.datakernel.util.ConcurrentStack;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.numberOfLeadingZeros;

public final class ByteBufPool {
	private static int minSize = 32;
	private static int maxSize = 1 << 30;

	/**
	 * Each array item contains collection (stack) of ByteBufs with specific size.
	 * For example slabs[0] contains ByteBufs with capacity 2^0 bytes,
	 * slabs[1] - ByteBufs with capacity 2^1,
	 * slabs[2] - ByteBufs with capacity 2^2 bytes and so on.
	 * Except slabs[32] that contains ByteBufs with size 0
	 */
	private static final ConcurrentStack<ByteBuf>[] slabs;

	static {
		//noinspection unchecked
		slabs = new ConcurrentStack[33];
		for (int i = 0; i < slabs.length; i++) {
			slabs[i] = new ConcurrentStack<>();
		}
	}

	/**
	 * Creates a new instance of ByteBufPool with min - minimal size of buffer to be stored,
	 * max - maximum size of buffer to be stored.
	 *
	 * @param minSize minimal size of buffer to be stored
	 * @param maxSize maximum size of buffer to be stored
	 */
	public static void setSizes(int minSize, int maxSize) {
		ByteBufPool.minSize = minSize;
		ByteBufPool.maxSize = maxSize;
	}

	/**
	 * Allocates a new byte buffer from this pool.
	 * The new buffer's position will be zero, its size will be its size and each of its elements will be
	 * initialized to zero.
	 *
	 * @param size new buffers size in bytes
	 * @return the new ByteBuf
	 */
	public static ByteBuf allocate(int size) {
		if (size < minSize || size >= maxSize) {
			return ByteBuf.allocate(size);
		}
		int index = 32 - numberOfLeadingZeros(size - 1); // index==32 for size==0
		ConcurrentStack<ByteBuf> queue = slabs[index];
		ByteBuf buf = queue.pop();
		if (buf != null) {
			buf.refs = 1;
			buf.position(0);
			buf.limit(size);
		} else {
			byte[] array = new byte[1 << index];
			buf = ByteBuf.wrap(array, 0, size);
			buf.refs = 1;
		}
		return buf;
	}

	/**
	 * Returns new buffer and recycles buffer from arguments.
	 *
	 * @param prevBuf the buffer to be reallocated
	 * @param newSize new size for buffer
	 * @return new buffer with new size
	 */
	public static ByteBuf reallocate(ByteBuf prevBuf, int newSize) {
		int prevSize = prevBuf.array().length;
		if (newSize <= prevSize && (prevSize <= minSize || numberOfLeadingZeros(newSize - 1) == numberOfLeadingZeros(prevSize - 1))) {
			prevBuf.position(0);
			prevBuf.limit(newSize);
			return prevBuf;
		}
		prevBuf.recycle();
		return allocate(newSize);
	}

	/**
	 * Allocates new buffer with new size, copies there all bytes from prevBuf and recycles it.
	 *
	 * @param prevBuf buffer to be resized
	 * @param newSize new size for buffer
	 * @return new buffer with bytes from prevBuf and with new size
	 */
	public static ByteBuf resize(ByteBuf prevBuf, int newSize) {
		if (newSize <= prevBuf.array().length) {
			prevBuf.limit(newSize);
			if (prevBuf.position() > newSize)
				prevBuf.position(newSize);
			return prevBuf;
		}
		ByteBuf newBuf = allocate(newSize);
		System.arraycopy(prevBuf.array(), 0, newBuf.array(), 0, prevBuf.limit());
		newBuf.position(prevBuf.position());
		prevBuf.recycle();
		return newBuf;
	}

	/**
	 * Appends two ByteBufs from arguments
	 *
	 * @param buf          result buffer
	 * @param dataToAppend buffer for appending
	 * @return result buffer
	 */
	public static ByteBuf append(ByteBuf buf, ByteBuf dataToAppend) {
		return append(buf, dataToAppend, dataToAppend.remaining());
	}

	/**
	 * Appends size elements after position from dataToAppend to buf.
	 *
	 * @param buf          result buffer
	 * @param dataToAppend buffer for appending
	 * @param size         number of elements to appending
	 * @return result buffer
	 */
	public static ByteBuf append(ByteBuf buf, ByteBuf dataToAppend, int size) {
		buf = append(buf, dataToAppend.array(), dataToAppend.position(), size);
		dataToAppend.advance(size);
		return buf;
	}

	/**
	 * Appends all elements from array from argument to buf.
	 *
	 * @param buf   result buffer
	 * @param array array for appending
	 * @return result buffer
	 */
	public static ByteBuf append(ByteBuf buf, byte[] array) {
		return append(buf, array, 0, array.length);
	}

	/**
	 * Appends the size elements after offset from the source array to buf.
	 *
	 * @param buf    result buffer
	 * @param array  array for appending
	 * @param offset starting position in the source array
	 * @param size   number of bytes for appending
	 * @return result buffer
	 */
	public static ByteBuf append(ByteBuf buf, byte[] array, int offset, int size) {
		int newPosition = buf.position() + size;
		if (newPosition > buf.array().length) {
			buf = resize(buf, newPosition);
		}
		System.arraycopy(array, offset, buf.array(), buf.position(), size);
		if (newPosition > buf.limit())
			buf.limit(newPosition);
		buf.position(newPosition);
		return buf;
	}

	/**
	 * Puts back ByteBuf which was taken from pool
	 * <p>
	 * Puts back the ByteBuf which was taking from pool
	 *
	 * @param buf ByteBuf for recycling
	 */
	protected static void recycle(ByteBuf buf) {
		assert buf.array.length >= minSize && buf.array.length <= maxSize;
		int index = 32 - numberOfLeadingZeros(buf.array.length - 1);
		ConcurrentStack<ByteBuf> queue = slabs[index];
		assert !queue.contains(buf) : "duplicate recycle array";
		queue.push(buf);
	}

	@VisibleForTesting
	static ConcurrentStack<ByteBuf>[] getPool() {
		return slabs;
	}

	/**
	 * Removes all items from this pool
	 */
	public static void clear() {
		for (ConcurrentStack<ByteBuf> slab : slabs) {
			slab.clear();
		}
	}

	/**
	 * Returns the number of items in this pool
	 */
	public static int getPoolItems() {
		int result = 0;
		for (ConcurrentStack<ByteBuf> slab : slabs) {
			result += slab.size();
		}
		return result;
	}

	/**
	 * Returns the number of occupied bytes in in this pool
	 */
	private static int getPoolSize() {
		int result = 0;
		for (int i = 0; i < slabs.length; i++) {
			int slotSize = 1 << i;
			result += slotSize * slabs[i].size();
		}
		return result;
	}

	/**
	 * Returns the size of this pool in kB
	 */
	public static int getPoolSizeKB() {
		return getPoolSize() / 1024;
	}

	/**
	 * Returns mean size of each item in this pool
	 */
	public static int getPoolItemAvgSize() {
		int items = getPoolItems();
		return items == 0 ? 0 : getPoolSize() / items;
	}

	/**
	 * Returns an array of String where each string described each slab in pool.
	 * It contains data about size of slot in slab, number of elements in slab, and size of slot in kB
	 */
	public static String[] getPoolSlabs() {
		String[] result = new String[slabs.length];
		for (int i = 0; i < slabs.length; i++) {
			long slotSize = 1L << i;
			int count = slabs[i].size();
			result[i] = slotSize + " : " + count + " = " + slotSize * count / 1024 + " kB";
		}
		return result;
	}

}