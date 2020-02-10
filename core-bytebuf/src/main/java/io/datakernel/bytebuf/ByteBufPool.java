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
import io.datakernel.common.ApplicationSettings;
import io.datakernel.common.MemSize;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.stream;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toList;

/**
 * Represents a pool of ByteBufs with 33 slabs. Each of these slabs
 * is a {@code ByteBufConcurrentStack} which stores ByteBufs of a
 * particular capacity which is a power of two.
 * <p>
 * When you need a new ByteBuf, it is either created (if a ByteBuf of
 * such capacity hasn't been used and recycled yet) or popped from the
 * appropriate slabs' stack.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public final class ByteBufPool {
	private static final int NUMBER_OF_SLABS = 33;

	/**
	 * Defines the minimal size of ByteBufs in this ByteBufPool.
	 * A constant value, by default set at 0.
	 */
	private static final int MIN_SIZE = ApplicationSettings.getMemSize(ByteBufPool.class, "minSize", MemSize.ZERO).toInt();

	/**
	 * Defines the maximum size of ByteBufs in this ByteBufPool.
	 * A constant value, by default set at 0.
	 */
	private static final int MAX_SIZE = ApplicationSettings.getMemSize(ByteBufPool.class, "maxSize", MemSize.ZERO).toInt();
	private static final boolean MIN_MAX_CHECKS = MIN_SIZE != 0 || MAX_SIZE != 0;

	/**
	 * Allows to get trace stack about {@code ByteBufs} of this {@code ByteBufPool}
	 * while debugging if set at value {@code true} (note that this is resource intensive).
	 * By default set at value {@code false}. If changed, significantly
	 * influences the performance and workflow of {@link #allocate(int)} operation.
	 */
	static final boolean REGISTRY = ApplicationSettings.getBoolean(ByteBufPool.class, "registry", false);

	/**
	 * Allows to get statistics about this ByteBufPool while debugging
	 * if set at value {@code true} (note that this is resource intensive).
	 * By default set at value {@code false}. If changed, significantly
	 * influences the performance and workflow of {@link #allocate(int)} operation.
	 */
	static final boolean STATS = ApplicationSettings.getBoolean(ByteBufPool.class, "stats", false);

	/**
	 * Allows to clear byte bufs when being returned to the pool
	 * if set at value {@code true} (note that this is resource intensive).
	 * By default set at value {@code false}. If changed,
	 * influences the performance of {@link #recycle(ByteBuf)} operation.
	 * <strong>Should only be used for testing to catch bugs with premature {@link ByteBuf} recycling,
	 * should not be used in production code</strong>
	 */
	static final boolean CLEAR_ON_RECYCLE = ApplicationSettings.getBoolean(ByteBufPool.class, "clearOnRecycle", false);

	/**
	 * {@code ByteBufConcurrentStack} allows to work with slabs and their ByteBufs.
	 * Basically, it is a singly linked list with basic stack operations:
	 * {@code push, pop, peek, clear, isEmpty, size}.
	 * <p>
	 * The implementation of {@code ByteBufConcurrentStack} is highly efficient
	 * due to utilizing {@link java.util.concurrent.atomic.AtomicReference}.
	 * Moreover, such approach allows to work with slabs concurrently safely.
	 */
	static final ByteBufConcurrentStack[] slabs;
	static final AtomicInteger[] created;
	static final AtomicInteger[] reused;

	private static final ByteBufPoolStats stats = new ByteBufPoolStats();

	/**
	 * Stores information about ByteBufs for stats.
	 * <p>
	 * It is a helper class which contains <i>size</i>,
	 * <i>timestamp</i> and <i>stackTrace</i> which represent
	 * information about the ByteBufs.
	 */
	public static final class Entry {
		final int size;
		final long timestamp;
		final Thread thread;
		final StackTraceElement[] stackTrace;

		Entry(int size, long timestamp, Thread thread, StackTraceElement[] stackTrace) {
			this.size = size;
			this.timestamp = timestamp;
			this.thread = thread;
			this.stackTrace = stackTrace;
		}

		public int getSize() {
			return size;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public String getAge() {
			return Duration.ofMillis(System.currentTimeMillis() - timestamp).toString();
		}

		public String getThread() {
			return thread.toString();
		}

		public List<String> getStackTrace() {
			return Arrays.stream(stackTrace).map(StackTraceElement::toString).collect(toList());
		}

		@Override
		public String toString() {
			return "{" +
					"size=" + size +
					", timestamp=" + timestamp +
					", thread=" + thread +
					", stackTrace=" + Arrays.toString(stackTrace) +
					'}';
		}
	}

	private static final WeakHashMap<ByteBuf, Entry> allocateRegistry = new WeakHashMap<>();
	private static final WeakHashMap<ByteBuf, Entry> recycleRegistry = new WeakHashMap<>();

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

	private ByteBufPool() {}

	/**
	 * Allocates byte buffer from the pool with size of
	 * <code>ceil(log<sub>2</sub>(size))<sup>2</sup></code>
	 * (rounds up to the nearest power of 2) bytes.
	 * <p>
	 * Note that resource intensive {@link #registerAllocate(ByteBuf)} (ByteBuf)} will be executed
	 * only if {@code #REGISTRY} is set {@code true}. Also, such parameters as
	 * {@code STATS}, {@code MIN_MAX_CHECKS}, {@code MIN_SIZE}, {@code MAX_SIZE}
	 * significantly influence the workflow of the {@code allocate} operation.
	 *
	 * @param size returned byte buffer size is guaranteed to be bigger or equal to requested size
	 * @return byte buffer from this pool
	 */
	@NotNull
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
			if (ByteBuf.CHECK_RECYCLE && buf.refs != -1) throw onByteBufRecycled(buf);
			buf.tail = 0;
			buf.head = 0;
			buf.refs = 1;
			if (STATS) recordReuse(index);
			if (REGISTRY) registerAllocate(buf);
		} else {
			buf = ByteBuf.wrapForWriting(new byte[1 << index]);
			buf.refs = 1;
			if (STATS) recordNew(index);
			if (REGISTRY) registerAllocate(buf);
		}
		return buf;
	}

	private static void recordNew(int index) {
		created[index].incrementAndGet();
	}

	private static void recordReuse(int index) {
		reused[index].incrementAndGet();
	}

	private static void registerAllocate(@NotNull ByteBuf buf) {
		Entry entry = buildRegistryEntry(buf);
		synchronized (allocateRegistry) {
			allocateRegistry.put(buf, entry);
		}
	}

	private static void registerRecycle(@NotNull ByteBuf buf) {
		Entry entry = buildRegistryEntry(buf);
		synchronized (recycleRegistry) {
			recycleRegistry.put(buf, entry);
		}
	}

	private static Entry buildRegistryEntry(@NotNull ByteBuf buf) {
		Thread thread = currentThread();
		StackTraceElement[] stackTrace = thread.getStackTrace();
		return new Entry(buf.array.length, currentTimeMillis(), thread,
				Arrays.copyOfRange(stackTrace, 4, stackTrace.length));
	}

	static AssertionError onByteBufRecycled(@NotNull ByteBuf buf) {
		int slab = 32 - numberOfLeadingZeros(buf.array.length - 1);
		ByteBufConcurrentStack stack = slabs[slab];
		stack.clear();
		return new AssertionError("Attempt to use recycled ByteBuf" +
				(REGISTRY ? ByteBufPool.getByteBufTrace(buf) : ""));
	}

	static String getByteBufTrace(@NotNull ByteBuf buf) {
		Entry allocated;
		Entry recycled;
		synchronized (allocateRegistry) {
			allocated = allocateRegistry.get(buf);
		}
		synchronized (recycleRegistry) {
			recycled = recycleRegistry.get(buf);
		}
		if (allocated == null && recycled == null) return "";
		return "\nAllocated: " + allocated +
				"\nRecycled: " + recycled;
	}

	/**
	 * Allocates byte buffer in the same way as {@link #allocate(int)} does, but
	 * sets its positions so that write-remaining is equal to requested size.
	 * <p>
	 * For example, if you need a {@code ByteBuf} of size 21,
	 * a {@code ByteBuf} of size 32 is allocated. (|______|)<br>
	 * But its read/write positions are set to 11 so that only last 21 are writable (|__####|)
	 * <p>
	 *
	 * @param size requested size
	 * @return byte buffer from this pool with appropriate positions set
	 */
	@NotNull
	public static ByteBuf allocateExact(int size) {
		ByteBuf buf = allocate(size);
		int d = buf.writeRemaining() - size;
		buf.tail(d);
		buf.head(d);
		return buf;
	}

	@NotNull
	public static ByteBuf allocate(@NotNull MemSize size) {
		return allocate(size.toInt());
	}

	@NotNull
	public static ByteBuf allocateExact(@NotNull MemSize size) {
		return allocateExact(size.toInt());
	}

	/**
	 * Returns provided ByteBuf to the ByteBufPool to the appropriate slab.
	 *
	 * @param buf the ByteBuf to be recycled
	 */
	static void recycle(@NotNull ByteBuf buf) {
		int slab = 32 - numberOfLeadingZeros(buf.array.length - 1);
		ByteBufConcurrentStack stack = slabs[slab];
		if (CLEAR_ON_RECYCLE) Arrays.fill(buf.array(), (byte) 0);
		stack.push(buf);
		if (REGISTRY) registerRecycle(buf);
	}

	@NotNull
	public static ByteBuf ensureWriteRemaining(@NotNull ByteBuf buf, int newWriteRemaining) {
		return ensureWriteRemaining(buf, 0, newWriteRemaining);
	}

	/**
	 * Checks if current ByteBuf can accommodate the needed
	 * amount of writable bytes.
	 * <p>
	 * Returns this ByteBuf, if it contains enough writable bytes.
	 * <p>
	 * Otherwise creates a new ByteBuf which contains data from the
	 * original ByteBuf and fits the parameters. Then recycles the
	 * original ByteBuf.
	 *
	 * @param buf               the ByteBuf to check
	 * @param minSize           the minimal size of the ByteBuf
	 * @param newWriteRemaining amount of needed writable bytes
	 * @return a ByteBuf which fits the parameters
	 */
	@NotNull
	public static ByteBuf ensureWriteRemaining(@NotNull ByteBuf buf, int minSize, int newWriteRemaining) {
		if (newWriteRemaining == 0) return buf;
		if (buf.writeRemaining() < newWriteRemaining || buf instanceof ByteBufSlice) {
			ByteBuf newBuf = allocate(max(minSize, newWriteRemaining + buf.readRemaining()));
			newBuf.put(buf);
			buf.recycle();
			return newBuf;
		}
		return buf;
	}

	/**
	 * Appends one ByteBuf to another ByteBuf. If target ByteBuf
	 * can't accommodate the ByteBuf to be appended, a new ByteBuf
	 * is created which contains both target and source ByteBufs data.
	 * The source ByteBuf is recycled after append.
	 * <p>
	 * If target ByteBuf has no readable bytes, it is being recycled
	 * and the source ByteBuf is returned.
	 * <p>
	 * Both ByteBufs must be not recycled before the operation.
	 *
	 * @param to   the target ByteBuf to which another ByteBuf will be appended
	 * @param from the source ByteBuf to be appended
	 * @return ByteBuf which contains the result of the appending
	 */
	@NotNull
	public static ByteBuf append(@NotNull ByteBuf to, @NotNull ByteBuf from) {
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

	/**
	 * Appends byte array to ByteBuf. If ByteBuf can't accommodate the
	 * byte array, a new ByteBuf is created which contains all data from
	 * the original ByteBuf and has enough capacity to accommodate the
	 * byte array.
	 * <p>
	 * ByteBuf must be not recycled before the operation.
	 *
	 * @param to     the target ByteBuf to which byte array will be appended
	 * @param from   the source byte array to be appended
	 * @param offset the value of offset for the byte array
	 * @param length amount of the bytes to be appended to the ByteBuf
	 *               The sum of the length and offset parameters can't
	 *               be greater than the whole length of the byte array
	 * @return ByteBuf which contains the result of the appending
	 */
	@NotNull
	public static ByteBuf append(@NotNull ByteBuf to, @NotNull byte[] from, int offset, int length) {
		assert !to.isRecycled();
		to = ensureWriteRemaining(to, length);
		to.put(from, offset, length);
		return to;
	}

	@NotNull
	public static ByteBuf append(@NotNull ByteBuf to, @NotNull byte[] from) {
		return append(to, from, 0, from.length);
	}

	/**
	 * Clears all of the slabs and stats.
	 */
	public static void clear() {
		for (int i = 0; i < ByteBufPool.NUMBER_OF_SLABS; i++) {
			slabs[i].clear();
			created[i].set(0);
			reused[i].set(0);
		}
		synchronized (allocateRegistry) {
			allocateRegistry.clear();
		}
		synchronized (recycleRegistry) {
			recycleRegistry.clear();
		}
	}

	@NotNull
	public static ByteBufPoolStats getStats() {
		return stats;
	}

	public interface ByteBufPoolStatsMXBean {
		int getCreatedItems();

		int getReusedItems();

		int getPoolItems();

		long getPoolSize();

		long getPoolSizeKB();

		List<String> getPoolSlabs();

		List<Entry> queryUnrecycledBufs(int limit);

		void clear();

		void clearRegistry();
	}

	/**
	 * Manages stats for this {@link ByteBufPool}. You can get the
	 * amount of created and reused ByteBufs and amount of ByteBufs
	 * stored in each of the slabs.
	 * <p>
	 * Also, you can get a String which contains information about
	 * amount of created and stored in pool ByteBufs.
	 * <p>
	 * For memory control, you can get the size of your ByteBufPool in
	 * Byte or KB as well as get information about the slabs themselves
	 * (size, amount of ByteBufs created, reused, stored in pool, and
	 * total size in KB) and unrecycled ByteBufs.
	 * <p>
	 * Finally, it allows to clear this ByteBufPool and its registry.
	 */
	public static final class ByteBufPoolStats implements ByteBufPoolStatsMXBean {
		@Override
		public int getCreatedItems() {
			return stream(created).mapToInt(AtomicInteger::get).sum();
		}

		@Override
		public int getReusedItems() {
			return stream(reused).mapToInt(AtomicInteger::get).sum();
		}

		@Override
		public int getPoolItems() {
			return stream(slabs).mapToInt(ByteBufConcurrentStack::size).sum();
		}

		public String getPoolItemsString() {
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

		@Override
		public long getPoolSize() {
			long result = 0;
			for (int i = 0; i < slabs.length - 1; i++) {
				long slabSize = 1L << i;
				result += slabSize * slabs[i].size();
			}
			return result;
		}

		@Override
		public long getPoolSizeKB() {
			return getPoolSize() / 1024;
		}

		public Map<ByteBuf, Entry> getUnrecycledBufs() {
			synchronized (allocateRegistry) {
				Map<ByteBuf, Entry> externalBufs = new IdentityHashMap<>(allocateRegistry);
				for (ByteBufConcurrentStack slab : slabs) {
					for (ByteBuf buf = slab.peek(); buf != null; buf = buf.next) {
						externalBufs.remove(buf);
					}
				}
				return externalBufs;
			}
		}

		@Override
		public List<Entry> queryUnrecycledBufs(int limit) {
			if (limit < 1) throw new IllegalArgumentException("Limit must be >= 1");
			Map<ByteBuf, Entry> danglingBufs = getUnrecycledBufs();
			return danglingBufs.values().stream().sorted(comparingLong(Entry::getTimestamp)).limit(limit).collect(toList());
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
			synchronized (allocateRegistry) {
				allocateRegistry.clear();
			}
			synchronized (recycleRegistry) {
				recycleRegistry.clear();
			}
		}
	}

	//endregion

}
