package io.datakernel.aio.buffer;

import io.datakernel.util.ApplicationSettings;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toList;

public final class DirectBufferPool {
	private static final int NUMBER_SLABS = 33;


	private static final int MIN_SIZE = ApplicationSettings.getInt(DirectBufferPool.class, "minSize", 0);

	private static final int MAX_SIZE = ApplicationSettings.getInt(DirectBufferPool.class, "maxSize", 0);
	private static final boolean MIN_MAX_CHECKS = MIN_SIZE != 0 || MAX_SIZE != 0;

	private static final boolean REGISTRY = ApplicationSettings.getBoolean(DirectBufferPool.class, "registry", false);

	private static final boolean STATS = ApplicationSettings.getBoolean(DirectBufferPool.class, "stats", false);

	private static final AtomicInteger[] created;
	private static final AtomicInteger[] reused;
	private static final DirectBufferConcurrentStack[] slabs;

	private static final DirectBufferPoolStats stats = new DirectBufferPoolStats();

	public static final class Entry {
		final int size;
		final long timestamp;
		final List<StackTraceElement> stackTrace;

		Entry(int size, long timestamp, List<StackTraceElement> stackTrace) {
			this.size = size;
			this.timestamp = timestamp;
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

		public List<String> getStackTrace() {
			return stackTrace.stream().map(StackTraceElement::toString).collect(toList());
		}
	}

	private static final WeakHashMap<LinkedDirectBuffer, Entry> registry = new WeakHashMap<>();

	static {
		slabs = new DirectBufferConcurrentStack[NUMBER_SLABS];
		created = new AtomicInteger[NUMBER_SLABS];
		reused = new AtomicInteger[NUMBER_SLABS];

		for (int i = 0; i < NUMBER_SLABS; i++) {
			slabs[i] = new DirectBufferConcurrentStack();
			created[i] = new AtomicInteger();
			reused[i] = new AtomicInteger();
		}
	}

	public static ByteBuffer allocate(int size) {
		if (MIN_MAX_CHECKS) {
			if ((MIN_SIZE != 0 && size < MIN_SIZE) || (MAX_SIZE != 0 && size >= MAX_SIZE)) {
				// not willing to register in pool
				return ByteBuffer.allocateDirect(size);
			}
		}

		int index = 32 - numberOfLeadingZeros(size - 1);
		DirectBufferConcurrentStack stack = slabs[index];
		LinkedDirectBuffer buffer = stack.pop();
		if (buffer != null) {
			buffer.buffer.clear();
			if (STATS) recordReuse(index);
			if (REGISTRY) register(buffer);
		} else {
			ByteBuffer newBuffer = ByteBuffer.allocateDirect(1 << index);
			newBuffer.order(ByteOrder.nativeOrder());
			buffer = new LinkedDirectBuffer(newBuffer);

			if (STATS) recordNew(index);
			if (REGISTRY) register(buffer);
		}

		return buffer.buffer;
	}

	private static void register(@NotNull LinkedDirectBuffer linkedBuffer) {
		ByteBuffer buffer = linkedBuffer.buffer;
		synchronized (registry) {
			StackTraceElement[] stackTrace = currentThread().getStackTrace();
			ArrayList<StackTraceElement> stackTraceList = new ArrayList<>(asList(stackTrace).subList(3, stackTrace.length));
			registry.put(linkedBuffer, new Entry(buffer.capacity(), currentTimeMillis(), stackTraceList));
		}
	}

	private static void recordNew(int index) {
		created[index].incrementAndGet();
	}

	private static void recordReuse(int index) {
		reused[index].incrementAndGet();
	}

	public static ByteBuffer allocateExact(int size) {
		ByteBuffer buffer = allocate(size);
		int allSize = buffer.remaining();
		buffer.position(allSize - size);

		return buffer;
	}

	public static void recycle(@NotNull ByteBuffer buffer) {
		int slab = 32 - numberOfLeadingZeros(buffer.capacity() - 1);
		DirectBufferConcurrentStack stack = slabs[slab];
		stack.push(new LinkedDirectBuffer(buffer));
	}

	public static void clear() {
		for (int i = 0; i < NUMBER_SLABS; i++) {
			slabs[i].clear();
			created[i].set(0);
			reused[i].set(0);
		}
		synchronized (registry) {
			registry.clear();
		}
	}

	@NotNull
	public static DirectBufferPoolStats getStats() {
		return stats;
	}

	public interface DirectBufferPoolStatsMXBean {
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

	public static final class DirectBufferPoolStats implements DirectBufferPoolStatsMXBean {
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
			return stream(slabs).mapToInt(DirectBufferConcurrentStack::size).sum();
		}

		public String getPoolItemsString() {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < NUMBER_SLABS; ++i) {
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

		public Map<ByteBuffer, Entry> getUnrecycledBufs() {
			synchronized (registry) {
				Map<ByteBuffer, Entry> externalBufs = new IdentityHashMap<>();
				registry.forEach((linkedBuffer, entry) -> externalBufs.put(linkedBuffer.buffer, entry));

				for (DirectBufferConcurrentStack slab : slabs) {
					LinkedDirectBuffer linkedBuffer = slab.peek();

					while (linkedBuffer != null) {
						externalBufs.remove(linkedBuffer.buffer);
						linkedBuffer = linkedBuffer.next;
					}
				}
				return externalBufs;
			}
		}

		@Override
		public List<Entry> queryUnrecycledBufs(int limit) {
			if (limit < 1) throw new IllegalArgumentException("Limit must be >= 1");
			Map<ByteBuffer,Entry> danglingBufs = getUnrecycledBufs();
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
			DirectBufferPool.clear();
		}

		@Override
		public void clearRegistry() {
			synchronized (registry) {
				registry.clear();
			}
		}
	}
}

