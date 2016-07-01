package io.datakernel.bytebuf;

import io.datakernel.jmx.MBeanFormat;
import io.datakernel.util.ConcurrentStack;

import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.Preconditions.check;
import static java.lang.Integer.numberOfLeadingZeros;

public class ByteBufPool {
	private static final int NUMBER_SLABS = 33;

	private static int minSize = 32;
	private static int maxSize = 1 << 30;

	private static final ConcurrentStack<ByteBuf>[] slabs = createSlabs(NUMBER_SLABS);
	private static final int[] created = new int[NUMBER_SLABS];

	// allocating
	public static ByteBuf allocateAtLeast(int size) {
		if (size < minSize || size >= maxSize) {
			return ByteBuf.create(size); // не хотим регистрировать в пуле
		}
		int index = 32 - numberOfLeadingZeros(size - 1); // index==32 for size==0
		ConcurrentStack<ByteBuf> queue = slabs[index];
		ByteBuf buf = queue.pop();
		if (buf != null) {
			buf.reset();
		} else {
			buf = ByteBuf.create(1 << index);
			buf.refs++;
			created[index]++;
		}
		return buf;
	}

	public static ByteBuf reallocateAtLeast(ByteBuf buf, int newSize) {
		assert !(buf instanceof ByteBuf.ByteBufSlice);
		int limit = buf.getLimit();
		if (limit >= newSize && (limit <= minSize || numberOfLeadingZeros(limit - 1) == numberOfLeadingZeros(newSize - 1))) {
			return buf;
		} else {
			ByteBuf newBuf = allocateAtLeast(newSize);
			newBuf.put(buf);
			buf.recycle();
			return newBuf;
		}
	}

	public static ByteBuf concat(ByteBuf buf1, ByteBuf buf2) {
		assert !buf1.isRecycled() && !buf2.isRecycled();
		if (buf1.remainingToWrite() < buf2.remainingToRead()) {
			ByteBuf newBuf = allocateAtLeast(buf1.remainingToRead() + buf2.remainingToRead());
			newBuf.put(buf1);
			buf1.recycle();
			buf1 = newBuf;
		}
		buf1.put(buf2);
		buf2.recycle();
		return buf1;
	}

	public static void recycle(ByteBuf buf) {
		assert buf.array.length >= minSize && buf.array.length <= maxSize;
		ConcurrentStack<ByteBuf> queue = slabs[32 - numberOfLeadingZeros(buf.array.length - 1)];
		assert !queue.contains(buf) : "duplicate recycle array";
		queue.push(buf);
	}

	public static ConcurrentStack<ByteBuf>[] getPool() {
		return slabs;
	}

	public static void clear() {
		for (int i = 0; i < ByteBufPool.NUMBER_SLABS; i++) {
			slabs[i].clear();
			created[i] = 0;
		}
	}

	/*inner*/
	private static ConcurrentStack<ByteBuf>[] createSlabs(int numberOfSlabs) {
		//noinspection unchecked
		ConcurrentStack<ByteBuf>[] slabs = new ConcurrentStack[numberOfSlabs];
		for (int i = 0; i < slabs.length; i++) {
			slabs[i] = new ConcurrentStack<>();
		}
		return slabs;
	}

	//region  +jmx
	public static final ObjectName JMX_NAME = MBeanFormat.name(ByteBufPool.class.getPackage().getName(), ByteBufPool.class.getSimpleName());

	private static final ByteBufPool.ByteBufNPoolStats stats = new ByteBufPool.ByteBufNPoolStats();

	public static ByteBufPool.ByteBufNPoolStats getStats() {
		return stats;
	}

	public static int getCreatedItems() {
		int items = 0;
		for (int n : created) {
			items += n;
		}
		return items;
	}

	public static int getCreatedItems(int slab) {
		check(slab >= 0 && slab < slabs.length);
		return created[slab];
	}

	public static int getPoolItems(int slab) {
		check(slab >= 0 && slab < slabs.length);
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
		for (int i = 0; i < ByteBufPool.NUMBER_SLABS; ++i) {
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

	public static void setSizes(int minSize, int maxSize) {
		ByteBufPool.minSize = minSize;
		ByteBufPool.maxSize = maxSize;
	}

	public interface ByteBufPoolStatsMXBean {

		int getCreatedItems();

		int getPoolItems();

		long getPoolItemAvgSize();

		long getPoolSizeKB();

		List<String> getPoolSlabs();
	}

	public static final class ByteBufNPoolStats implements ByteBufPool.ByteBufPoolStatsMXBean {

		@Override
		public int getCreatedItems() {
			return ByteBufPool.getCreatedItems();
		}

		@Override
		public int getPoolItems() {
			return ByteBufPool.getPoolItems();
		}

		@Override
		public long getPoolItemAvgSize() {
			int result = 0;
			for (ConcurrentStack<ByteBuf> slab : slabs) {
				result += slab.size();
			}
			int items = result;
			return items == 0 ? 0 : ByteBufPool.getPoolSize() / items;
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
	}
	//endregion
}
