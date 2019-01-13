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

package io.datakernel.bytebuf;

import io.datakernel.exception.InvalidSizeException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.util.Recyclable;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.stream.Collector;

import static io.datakernel.util.CollectionUtils.emptyIterator;
import static java.lang.System.arraycopy;

@SuppressWarnings({"unused", "WeakerAccess"})
public final class ByteBufQueue implements Recyclable {
	private static final int DEFAULT_CAPACITY = 8;

	@NotNull
	private ByteBuf[] bufs;

	private int first = 0;
	private int last = 0;

	public ByteBufQueue() {
		this(DEFAULT_CAPACITY);
	}

	public ByteBufQueue(int capacity) {
		this.bufs = new ByteBuf[capacity];
	}

	private static Collector<ByteBuf, ByteBufQueue, ByteBuf> COLLECTOR = Collector.of(
			ByteBufQueue::new,
			ByteBufQueue::add,
			(bufs1, bufs2) -> { throw new UnsupportedOperationException();},
			ByteBufQueue::takeRemaining);

	public static Collector<ByteBuf, ByteBufQueue, ByteBuf> collector() {
		return COLLECTOR;
	}

	public static Collector<ByteBuf, ByteBufQueue, ByteBuf> collector(int maxSize) {
		return Collector.of(
				ByteBufQueue::new,
				(queue, buf) -> {
					int size = buf.readRemaining();
					if (size > maxSize || queue.hasRemainingBytes(maxSize - size + 1)) {
						queue.recycle();
						buf.recycle();
						throw new UncheckedException(new InvalidSizeException(ByteBufQueue.class,
								"ByteBufQueue exceeds maximum size of " + maxSize + " bytes"));
					}
					queue.add(buf);
				},
				(bufs1, bufs2) -> { throw new UnsupportedOperationException();},
				ByteBufQueue::takeRemaining);
	}

	private int next(int i) {
		return (i + 1) % bufs.length;
	}

	private void doPoll() {
		bufs[first].recycle();
		first = next(first);
	}

	private void grow() {
		ByteBuf[] newBufs = new ByteBuf[bufs.length * 2];
		arraycopy(bufs, last, newBufs, 0, bufs.length - last);
		arraycopy(bufs, 0, newBufs, bufs.length - last, last);
		first = 0;
		last = bufs.length;
		bufs = newBufs;
	}

	public void add(@NotNull ByteBuf buf) {
		if (!buf.canRead()) {
			buf.recycle();
			return;
		}

		bufs[last] = buf;
		last = next(last);
		if (last == first) {
			grow();
		}
	}

	public void addAll(@NotNull Iterable<ByteBuf> byteBufs) {
		for (ByteBuf buf : byteBufs) {
			add(buf);
		}
	}

	@NotNull
	public ByteBuf take() {
		assert hasRemaining();
		ByteBuf buf = bufs[first];
		first = next(first);
		return buf;
	}

	@Nullable
	public ByteBuf poll() {
		if (hasRemaining()) {
			return take();
		}
		return null;
	}

	/**
	 * Creates and returns ByteBufSlice that contains {@code size} bytes from queue's first ByteBuf
	 * if latter contains enough bytes. Otherwise creates and returns ByteBuf that contains all bytes
	 * from first ByteBuf in queue.
	 *
	 * @param size number of bytes to returning
	 * @return ByteBuf with result bytes
	 */
	@NotNull
	public ByteBuf takeAtMost(int size) {
		assert hasRemaining();
		ByteBuf buf = bufs[first];
		if (size >= buf.readRemaining()) {
			first = next(first);
			return buf;
		}
		ByteBuf result = buf.slice(size);
		buf.moveReadPosition(size);
		return result;
	}

	@NotNull
	public ByteBuf takeAtLeast(int size) {
		assert hasRemainingBytes(size);
		if (size == 0) return ByteBuf.empty();
		ByteBuf buf = bufs[first];
		if (buf.readRemaining() >= size) {
			first = next(first);
			return buf;
		}
		ByteBuf result = ByteBufPool.allocate(size);
		drainTo(result.array(), 0, size);
		result.moveWritePosition(size);
		return result;
	}

	@NotNull
	public ByteBuf takeAtLeast(int size, @NotNull ByteBufConsumer recycled) {
		assert hasRemainingBytes(size);
		if (size == 0) return ByteBuf.empty();
		ByteBuf buf = bufs[first];
		if (buf.readRemaining() >= size) {
			first = next(first);
			return buf;
		}
		ByteBuf result = ByteBufPool.allocate(size);
		drainTo(result.array(), 0, size, recycled);
		result.moveWritePosition(size);
		return result;
	}

	/**
	 * Returns ByteBuf that contains {@code exactSize} of bytes if queue has enough bytes.
	 * Otherwise returns ByteBuf that contains all bytes from queue
	 *
	 * @param exactSize amount of bytes to return
	 * @return ByteBuf with {@code exactSize} or less bytes
	 */
	@NotNull
	public ByteBuf takeExactSize(int exactSize) {
		assert hasRemainingBytes(exactSize);
		if (exactSize == 0) return ByteBuf.empty();
		ByteBuf buf = bufs[first];
		if (buf.readRemaining() == exactSize) {
			first = next(first);
			return buf;
		} else if (exactSize < buf.readRemaining()) {
			ByteBuf result = buf.slice(exactSize);
			buf.moveReadPosition(exactSize);
			return result;
		}
		ByteBuf result = ByteBufPool.allocate(exactSize);
		drainTo(result.array(), 0, exactSize);
		result.moveWritePosition(exactSize);
		return result;
	}

	@NotNull
	public ByteBuf takeExactSize(int exactSize, @NotNull ByteBufConsumer recycledBufs) {
		assert hasRemainingBytes(exactSize);
		if (exactSize == 0) return ByteBuf.empty();
		ByteBuf buf = bufs[first];
		if (buf.readRemaining() == exactSize) {
			first = next(first);
			return buf;
		} else if (exactSize < buf.readRemaining()) {
			ByteBuf result = buf.slice(exactSize);
			buf.moveReadPosition(exactSize);
			return result;
		}
		ByteBuf result = ByteBufPool.allocate(exactSize);
		drainTo(result.array(), 0, exactSize, recycledBufs);
		result.moveWritePosition(exactSize);
		return result;
	}

	public void consume(int size, @NotNull ByteBufConsumer consumer) {
		assert hasRemainingBytes(size);
		ByteBuf buf = bufs[first];
		if (buf.readRemaining() >= size) {
			int newPos = buf.readPosition() + size;
			consumer.accept(buf);
			buf.readPosition(newPos);
			if (!buf.canRead()) {
				first = next(first);
				buf.recycle();
			}
		} else {
			buf = ByteBufPool.allocate(size);
			drainTo(buf, size);
			consumer.accept(buf);
			buf.recycle();
		}
	}

	/**
	 * Creates and returns ByteBuf with all remaining bytes from queue
	 *
	 * @return ByteBuf with all remaining bytes
	 */
	@NotNull
	public ByteBuf takeRemaining() {
		return takeExactSize(remainingBytes());
	}

	/**
	 * Returns the first ByteBuf from this queue
	 */
	@Nullable
	@Contract(pure = true)
	public ByteBuf peekBuf() {
		return hasRemaining() ? bufs[first] : null;
	}

	/**
	 * Returns the ByteBuf with the given index,  relatively than head of queue
	 *
	 * @param n index of ByteBuf relatively than head of queue
	 */
	@NotNull
	@Contract(pure = true)
	public ByteBuf peekBuf(int n) {
		assert n <= remainingBufs();
		int i = first + n;
		if (i >= bufs.length)
			i -= bufs.length;
		return bufs[i];
	}

	/**
	 * Returns the number of ByteBufs in this queue
	 */
	@Contract(pure = true)
	public int remainingBufs() {
		return last >= first ? last - first : bufs.length + (last - first);
	}

	/**
	 * Returns the number of bytes in this queue
	 */
	@Contract(pure = true)
	public int remainingBytes() {
		int result = 0;
		for (int i = first; i != last; i = next(i)) {
			result += bufs[i].readRemaining();
		}
		return result;
	}

	/**
	 * Tells whether or not this queue is empty.
	 *
	 * @return true if, and only if, there is at least one element is remaining in this queue
	 */
	@Contract(pure = true)
	public boolean isEmpty() {
		return !hasRemaining();
	}

	@Contract(pure = true)
	public boolean hasRemaining() {
		return first != last;
	}

	/**
	 * Tells whether or not this queue has remaining bytes.
	 *
	 * @param remaining number of bytes for checking
	 * @return true if, and only if, there are remaining bytes.
	 */
	@Contract(pure = true)
	public boolean hasRemainingBytes(int remaining) {
		assert remaining >= 0;
		if (remaining == 0) return true;
		for (int i = first; i != last; i = next(i)) {
			int bufRemaining = bufs[i].readRemaining();
			if (bufRemaining >= remaining)
				return true;
			remaining -= bufRemaining;
		}
		return false;
	}

	/**
	 * Returns the first byte from this queue and removes it.
	 */
	public byte getByte() {
		assert hasRemaining();
		ByteBuf buf = bufs[first];
		assert buf.canRead();
		byte result = buf.get();
		if (!buf.canRead()) {
			doPoll();
		}
		return result;
	}

	/**
	 * Returns the first byte from this queue without its removing.
	 */
	@Contract(pure = true)
	public byte peekByte() {
		assert hasRemaining();
		ByteBuf buf = bufs[first];
		return buf.peek();
	}

	/**
	 * Returns the byte from this queue with the given index
	 *
	 * @param index the index at which the bytes will be returned
	 */
	@Contract(pure = true)
	public byte peekByte(int index) {
		assert hasRemainingBytes(index + 1);
		for (int i = first; ; i = next(i)) {
			ByteBuf buf = bufs[i];
			if (index < buf.readRemaining())
				return buf.peek(index);
			index -= buf.readRemaining();
		}
	}

	/**
	 * Removes {@code maxSize} bytes from this queue
	 *
	 * @param maxSize number of bytes for removing
	 * @return number of removed bytes
	 */
	public int skip(int maxSize) {
		int s = maxSize;
		while (hasRemaining()) {
			ByteBuf buf = bufs[first];
			int remaining = buf.readRemaining();
			if (s < remaining) {
				buf.moveReadPosition(s);
				return maxSize;
			} else {
				buf.recycle();
				first = next(first);
				s -= remaining;
			}
		}
		return maxSize - s;
	}

	public int skip(int maxSize, @NotNull ByteBufConsumer recycledBufs) {
		int s = maxSize;
		while (hasRemaining()) {
			ByteBuf buf = bufs[first];
			int remaining = buf.readRemaining();
			if (s < remaining) {
				buf.moveReadPosition(s);
				return maxSize;
			} else {
				recycledBufs.accept(buf);
				buf.recycle();
				first = next(first);
				s -= remaining;
			}
		}
		return maxSize - s;
	}

	/**
	 * Adds {@code maxSize} bytes from this queue to dest if queue contains more than {@code maxSize} bytes.
	 * Otherwise adds all bytes from queue to dest. In both cases advances queue's position to number of drained bytes.
	 *
	 * @param dest       array to draining
	 * @param destOffset start position for adding to dest
	 * @param maxSize    number of bytes for adding
	 * @return number of drained bytes.
	 */
	public int drainTo(@NotNull byte[] dest, int destOffset, int maxSize) {
		int s = maxSize;
		while (hasRemaining()) {
			ByteBuf buf = bufs[first];
			int remaining = buf.readRemaining();
			if (s < remaining) {
				arraycopy(buf.array(), buf.readPosition(), dest, destOffset, s);
				buf.moveReadPosition(s);
				return maxSize;
			} else {
				arraycopy(buf.array(), buf.readPosition(), dest, destOffset, remaining);
				buf.recycle();
				first = next(first);
				s -= remaining;
				destOffset += remaining;
			}
		}
		return maxSize - s;
	}

	public int drainTo(@NotNull byte[] dest, int destOffset, int maxSize, @NotNull ByteBufConsumer recycledBufs) {
		int s = maxSize;
		while (hasRemaining()) {
			ByteBuf buf = bufs[first];
			int remaining = buf.readRemaining();
			if (s < remaining) {
				arraycopy(buf.array(), buf.readPosition(), dest, destOffset, s);
				buf.moveReadPosition(s);
				return maxSize;
			} else {
				arraycopy(buf.array(), buf.readPosition(), dest, destOffset, remaining);
				recycledBufs.accept(buf);
				buf.recycle();
				first = next(first);
				s -= remaining;
				destOffset += remaining;
			}
		}
		return maxSize - s;
	}

	/**
	 * Adds {@code maxSize} bytes from this queue to ByteBuf dest if queue contains more than {@code maxSize} bytes.
	 * Otherwise adds all bytes from queue to dest. In both cases advances queue's position to number of drained bytes.
	 *
	 * @param dest    ByteBuf for draining
	 * @param maxSize number of bytes for adding
	 * @return number of drained bytes.
	 */
	public int drainTo(@NotNull ByteBuf dest, int maxSize) {
		int actualSize = drainTo(dest.array(), dest.writePosition(), maxSize);
		dest.moveWritePosition(actualSize);
		return actualSize;
	}

	/**
	 * Adds as much bytes to dest as it can store. If queue doesn't contain enough bytes - adds all byte from queue.
	 * Advances queue's position to number of drained bytes.
	 *
	 * @param dest ByteBuf for draining
	 * @return number of drained bytes
	 */
	public int drainTo(@NotNull ByteBuf dest) {
		return drainTo(dest, dest.writeRemaining());
	}

	/**
	 * Copies all bytes from this queue to dest, and removes it from this queue.
	 *
	 * @param dest ByteBufQueue  for draining
	 * @return number of adding bytes
	 */
	public int drainTo(@NotNull ByteBufQueue dest) {
		int size = 0;
		while (hasRemaining()) {
			ByteBuf buf = take();
			dest.add(buf);
			size += buf.readRemaining();
		}
		return size;
	}

	public int drainTo(@NotNull ByteBufConsumer dest) {
		int size = 0;
		while (hasRemaining()) {
			ByteBuf buf = take();
			dest.accept(buf);
			size += buf.readRemaining();
		}
		return size;
	}

	/**
	 * Adds to ByteBufQueue dest {@code maxSize} bytes from this queue. If this queue doesn't contain enough bytes,
	 * add all bytes from this queue.
	 *
	 * @param dest    ByteBufQueue for draining
	 * @param maxSize number of bytes for adding
	 * @return number of added elements
	 */
	public int drainTo(@NotNull ByteBufQueue dest, int maxSize) {
		int s = maxSize;
		while (s != 0 && hasRemaining()) {
			ByteBuf buf = takeAtMost(s);
			dest.add(buf);
			s -= buf.readRemaining();
		}
		return maxSize - s;
	}

	public int drainTo(@NotNull ByteBufConsumer dest, int maxSize) {
		int s = maxSize;
		while (s != 0 && hasRemaining()) {
			ByteBuf buf = takeAtMost(s);
			dest.accept(buf);
			s -= buf.readRemaining();
		}
		return maxSize - s;
	}

	@NotNull
	public Iterator<ByteBuf> asIterator() {
		if (!hasRemaining()) return emptyIterator();
		ByteBufIterator iterator = new ByteBufIterator(this);
		first = last = 0;
		return iterator;
	}

	public static class ByteBufIterator implements Iterator<ByteBuf>, Recyclable {
		@NotNull
		final ByteBuf[] bufs;
		int first;
		final int last;

		private ByteBufIterator(@NotNull ByteBufQueue queue) {
			bufs = queue.bufs;
			first = queue.first;
			last = queue.last;
		}

		@Override
		public boolean hasNext() {
			return first != last;
		}

		@Override
		@NotNull
		public ByteBuf next() {
			ByteBuf buf = bufs[first];
			first = (first + 1) % bufs.length;
			return buf;
		}

		@Override
		public void recycle() {
			while (hasNext()) {
				next().recycle();
			}
		}
	}

	/**
	 * Recycles all ByteBufs from this queue.
	 */
	@Override
	public void recycle() {
		for (int i = first; i != last; i = next(i)) {
			bufs[i].recycle();
		}
		first = last = 0;
	}

	@Override
	public String toString() {
		return "bufs:" + remainingBufs() + " bytes:" + remainingBytes();
	}
}
