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
import io.datakernel.util.MemSize;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;

public final class ByteBufPool {
	private static final int NUMBER_OF_SLABS = 33;
	private static final int MIN_SIZE = ApplicationSettings.getInt(ByteBufPool.class, "minSize", 0);
	private static final int MAX_SIZE = ApplicationSettings.getInt(ByteBufPool.class, "maxSize", 0);
	private static final boolean MIN_MAX_CHECKS = MIN_SIZE != 0 || MAX_SIZE != 0;

	private static final boolean REGISTRY = ApplicationSettings.getBoolean(ByteBufPool.class, "registry", false);
	private static final boolean STATS = ApplicationSettings.getBoolean(ByteBufPool.class, "stats", false);

	private static final ByteBufConcurrentStack[] slabs;
	private static final AtomicInteger[] reused;
	private static final AtomicInteger[] created;

	private static class Entry {
		long timestamp;
		List<StackTraceElement> stackTrace;

		Entry(long timestamp, List<StackTraceElement> stackTrace) {
			this.timestamp = timestamp;
			this.stackTrace = stackTrace;
		}
	}

	private static final WeakHashMap<ByteBuf, Entry> registry = new WeakHashMap<>();

	static {
		slabs = new ByteBufConcurrentStack[NUMBER_OF_SLABS];
		created = new AtomicInteger[NUMBER_OF_SLABS];
		reused = new AtomicInteger[NUMBER_OF_SLABS];
		for (int i = 0; i < NUMBER_OF_SLABS; i++) {
			slabs[i] = new ByteBufConcurrentStack();
			created[i] = new AtomicInteger();
			reused[i] = new AtomicInteger();
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
		if (MIN_MAX_CHECKS) {
			if ((MIN_SIZE != 0 && size < MIN_SIZE) || (MAX_SIZE != 0 && size >= MAX_SIZE)) {
				// not willing to register in pool
				return ByteBuf.wrapForWriting(new byte[size]);
			}
		}
		int index = 32 - numberOfLeadingZeros(size - 1); // index==32 for size==0
		ByteBufConcurrentStack stack = slabs[index];
		ByteBuf buf = stack.pop();
		if (buf != null) {
			buf.reset();
			if (STATS) recordReuse(index);
			if (REGISTRY) registerReuse(buf);
		} else {
			buf = ByteBuf.wrapForWriting(new byte[1 << index]);
			buf.refs++;
			if (STATS) recordNew(index);
			if (REGISTRY) registerNew(buf);
		}
		return buf;
	}

	private static void recordNew(int index) {
		created[index].incrementAndGet();
	}

	private static void recordReuse(int index) {
		reused[index].incrementAndGet();
	}

	private static void registerNew(ByteBuf buf) {
		synchronized (registry) {
			StackTraceElement[] stackTrace = currentThread().getStackTrace();
			registry.put(buf, new Entry(currentTimeMillis(), asList(stackTrace).subList(3, stackTrace.length)));
		}
	}

	private static void registerReuse(ByteBuf buf) {
		synchronized (registry) {
			synchronized (registry) {
				Entry entry = registry.get(buf);
				if (entry == null) return;
				entry.timestamp = currentTimeMillis();
			}
		}
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
		ByteBufConcurrentStack stack = slabs[slab];
		stack.push(buf);
	}

	public static ByteBufConcurrentStack[] getSlabs() {
		return slabs;
	}

	public static void clear() {
		for (int i = 0; i < ByteBufPool.NUMBER_OF_SLABS; i++) {
			slabs[i].clear();
			created[i].set(0);
			reused[i].set(0);
		}
		clearRegistry();
	}

	public static void clearRegistry() {
		synchronized (registry) {
			registry.clear();
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
		return stream(created).mapToInt(AtomicInteger::get).sum();
	}

	public static int getReusedItems() {
		return stream(reused).mapToInt(AtomicInteger::get).sum();
	}

	public static int getPoolItems() {
		return stream(slabs).mapToInt(ByteBufConcurrentStack::size).sum();
	}

	public static String getPoolItemsString() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < ByteBufPool.NUMBER_OF_SLABS; ++i) {
			int createdItems = created[i].get();
			int poolItems = slabs[i].size();
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

	public Map<ByteBuf, Entry> getExternalBufs() {
		synchronized (registry) {
			Map<ByteBuf, Entry> externalBufs = new IdentityHashMap<>(registry);
			for (ByteBufConcurrentStack slab : slabs) {
				for (ByteBuf buf = slab.peek(); buf != null; buf = buf.next) {
					externalBufs.remove(buf);
				}
			}
			return externalBufs;
		}
	}

	public interface ByteBufPoolStatsMXBean {
		int getCreatedItems();

		int getReusedItems();

		int getPoolItems();

		long getPoolSizeKB();

		List<String> getPoolSlabs();

		void clear();

		void clearRegistry();
	}

	public static final class ByteBufPoolStats implements ByteBufPoolStatsMXBean {
		@Override
		public int getCreatedItems() {
			return ByteBufPool.getCreatedItems();
		}

		@Override
		public int getReusedItems() {
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
			assert slabs.length == 33;
			List<String> result = new ArrayList<>(slabs.length + 1);
			result.add("SlotSize,Created,Reused,InPool,Total(Kb)");
			for (int i = 0; i < slabs.length; i++) {
				int idx = (i + 32) % slabs.length;
				long slabSize = idx == 32 ? 0 : 1L << idx;
				int count = slabs[idx].size();
				result.add(slabSize + "," +
						(STATS ? created[idx] : '-') + "," +
						(STATS ? reused[idx] : '-') + "," +
						count + "," +
						slabSize * count / 1024);
			}
			return result;
		}

		@Override
		public void clear() {
			ByteBufPool.clear();
		}

		@Override
		public void clearRegistry() {
			ByteBufPool.clearRegistry();
		}
	}

	//endregion

}
