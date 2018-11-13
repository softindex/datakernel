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

import io.datakernel.bytebuf.ByteBuf.ByteBufSlice;
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.ConcurrentStack;
import io.datakernel.util.MemSize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Math.max;

public final class ByteBufPool {
	private static final int NUMBER_OF_SLABS = 33;
	private static final int minSize = ApplicationSettings.getInt(ByteBufPool.class, "minSize", 32);
	private static final int maxSize = ApplicationSettings.getInt(ByteBufPool.class, "maxSize", 1 << 30);

	private static final ConcurrentStack<ByteBuf>[] slabs;
	private static final AtomicInteger[] created;

	static {
		//noinspection unchecked
		slabs = new ConcurrentStack[NUMBER_OF_SLABS];
		created = new AtomicInteger[NUMBER_OF_SLABS];
		for (int i = 0; i < NUMBER_OF_SLABS; i++) {
			slabs[i] = new ConcurrentStack<>();
			created[i] = new AtomicInteger();
		}
	}

	private ByteBufPool() {
	}

	/**
	 * Allocates byte buffer from the pool with size of
	 * <code>ceil(log<sub>2</sub>(size))<sup>2</sup></code> (rounds up to nearest power of 2) bytes.
	 *
	 * @param size returned byte buffer size is guaranteed to be bigger or equal to requested size.
	 * @return byte buffer from this pool
	 */
	public static ByteBuf allocate(int size) {
		if ((minSize != 0 && size < minSize) || (maxSize != 0 && size >= maxSize)) {
			// not willing to register in pool
			return ByteBuf.wrapForWriting(new byte[size]);
		}
		int index = 32 - numberOfLeadingZeros(size - 1); // index==32 for size==0
		ConcurrentStack<ByteBuf> stack = slabs[index];
		ByteBuf buf = stack.pop();
		if (buf != null) {
			buf.reset();
		} else {
			buf = ByteBuf.wrapForWriting(new byte[1 << index]);
			buf.refs++;
			assert created[index].incrementAndGet() != Integer.MAX_VALUE;
		}
		return buf;
	}

	/**
	 * Allocates byte buffer in same way as {@link #allocate(int)} does, but sets its positions such that
	 * write-remaining is equal to requested size.
	 * <p>
	 * For example for size 21 byte buffer of size 32 is allocated                  (|______|)<br>
	 * But its read/write positions are set to 11 so that only last 21 are writable (|__####|)
	 *
	 * @param size requested size
	 * @return byte buffer from this pool with appropriate positions set
	 */
	public static ByteBuf allocateExact(int size) {
		ByteBuf buf = allocate(size);
		int d = buf.writeRemaining() - size;
		buf.writePosition(d);
		buf.readPosition(d);
		return buf;
	}

	public static ByteBuf allocate(MemSize size) {
		return allocate(size.toInt());
	}

	public static ByteBuf allocateExact(MemSize size) {
		return allocateExact(size.toInt());
	}

	public static void recycle(ByteBuf buf) {
		int slab = 32 - numberOfLeadingZeros(buf.array.length - 1);
		ConcurrentStack<ByteBuf> stack = slabs[slab];
		stack.push(buf);
	}

	public static ByteBuf recycleIfEmpty(ByteBuf buf) {
		if (buf.canRead())
			return buf;
		buf.recycle();
		return ByteBuf.empty();
	}

	public static ConcurrentStack<ByteBuf>[] getSlabs() {
		return slabs;
	}

	public static void clear() {
		for (int i = 0; i < ByteBufPool.NUMBER_OF_SLABS; i++) {
			slabs[i].clear();
			created[i].set(0);
		}
	}

	public static ByteBuf ensureWriteRemaining(ByteBuf buf, int newWriteRemaining) {
		return ensureWriteRemaining(buf, 0, newWriteRemaining);
	}

	public static ByteBuf ensureWriteRemaining(ByteBuf buf, int minSize, int newWriteRemaining) {
		if (newWriteRemaining == 0) return buf;
		if (buf.writeRemaining() < newWriteRemaining || buf instanceof ByteBufSlice) {
			ByteBuf newBuf = allocate(max(minSize, newWriteRemaining + buf.readRemaining()));
			newBuf.put(buf);
			buf.recycle();
			return newBuf;
		}
		return buf;
	}

	public static ByteBuf append(ByteBuf to, ByteBuf from) {
		assert !to.isRecycled() && !from.isRecycled();
		if (to.readRemaining() == 0) {
			to.recycle();
			return from;
		}
		to = ensureWriteRemaining(to, from.readRemaining());
		to.put(from);
		from.recycle();
		return to;
	}

	public static ByteBuf append(ByteBuf to, byte[] from, int offset, int length) {
		assert !to.isRecycled();
		to = ensureWriteRemaining(to, length);
		to.put(from, offset, length);
		return to;
	}

	public static ByteBuf append(ByteBuf to, byte[] from) {
		return append(to, from, 0, from.length);
	}

	private static final ByteBufPoolStats stats = new ByteBufPoolStats();

	public static ByteBufPoolStats getStats() {
		return stats;
	}

	public static int getCreatedItems() {
		int items = 0;
		for (AtomicInteger counter : created) {
			items += counter.get();
		}
		return items;
	}

	public static int getCreatedItems(int slab) {
		return created[slab].get();
	}

	public static int getPoolItems(int slab) {
		return slabs[slab].size();
	}

	public static int getPoolItems() {
		int result = 0;
		for (ConcurrentStack<ByteBuf> slab : slabs) {
			result += slab.size();
		}
		return result;
	}

	public static String getPoolItemsString() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < ByteBufPool.NUMBER_OF_SLABS; ++i) {
			int createdItems = ByteBufPool.getCreatedItems(i);
			int poolItems = ByteBufPool.getPoolItems(i);
			if (createdItems != poolItems) {
				sb.append(String.format("Slab %d (%d) ", i, (1 << i)))
						.append(" created: ").append(createdItems)
						.append(" pool: ").append(poolItems).append("\n");
			}
		}
		return sb.toString();
	}

	private static long getPoolSize() {
		assert slabs.length == 33 : "Except slabs[32] that contains ByteBufs with size 0";
		long result = 0;
		for (int i = 0; i < slabs.length - 1; i++) {
			long slotSize = 1L << i;
			result += slotSize * slabs[i].size();
		}
		return result;
	}

	public interface ByteBufPoolStatsMXBean {
		int getCreatedItems();

		int getPoolItems();

		long getPoolSizeKB();

		List<String> getPoolSlabs();

		void clearPool();
	}

	public static final class ByteBufPoolStats implements ByteBufPoolStatsMXBean {
		@Override
		public int getCreatedItems() {
			return ByteBufPool.getCreatedItems();
		}

		@Override
		public int getPoolItems() {
			return ByteBufPool.getPoolItems();
		}

		@Override
		public long getPoolSizeKB() {
			return ByteBufPool.getPoolSize() / 1024;
		}

		@Override
		public List<String> getPoolSlabs() {
			assert slabs.length == 33 : "Except slabs[32] that contains ByteBufs with size 0";
			List<String> result = new ArrayList<>(slabs.length + 1);
			result.add("SlotSize,Created,InPool,Total(Kb)");
			for (int i = 0; i < slabs.length; i++) {
				long slotSize = 1L << i;
				int count = slabs[i].size();
				result.add((slotSize & 0xffffffffL) + "," + created[i] + "," + count + "," + slotSize * count / 1024);
			}
			return result;
		}

		@Override
		public void clearPool() {
			ByteBufPool.clear();
		}
	}

	private static String formatHours(long period) {
		long milliseconds = period % 1000;
		long seconds = (period / 1000) % 60;
		long minutes = (period / (60 * 1000)) % 60;
		long hours = period / (60 * 60 * 1000);
		return String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", seconds) + "." + String.format("%03d", milliseconds);
	}

	public static String formatDuration(long period) {
		if (period == 0)
			return "";
		return formatHours(period);
	}

	private static String extractContent(ByteBuf buf, int maxSize) {
		int to = buf.readPosition() + Math.min(maxSize, buf.readRemaining());
		return new String(Arrays.copyOfRange(buf.array(), buf.readPosition(), to));
	}
	//endregion
}
