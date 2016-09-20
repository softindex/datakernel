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

import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.numberOfLeadingZeros;

public final class ByteBufPool {
	private static final int NUMBER_SLABS = 33;

	private static int minSize = 32;
	private static int maxSize = 1 << 30;

	private static final ConcurrentStack<ByteBuf>[] slabs = createSlabs(NUMBER_SLABS);
	private static final int[] created = new int[NUMBER_SLABS];

	private ByteBufPool() {}

	public static ByteBuf allocate(int size) {
		if (size < minSize || size >= maxSize) {
			// not willing to register in pool
			return ByteBuf.wrapForWriting(new byte[size]);
		}
		int index = 32 - numberOfLeadingZeros(size - 1); // index==32 for size==0
		ConcurrentStack<ByteBuf> queue = slabs[index];
		ByteBuf buf = queue.pop();
		if (buf != null) {
			buf.reset();
		} else {
			buf = ByteBuf.wrapForWriting(new byte[1 << index]);
			buf.refs++;
			created[index]++;
		}
		return buf;
	}

	public static void recycle(ByteBuf buf) {
		assert buf.array.length >= minSize && buf.array.length <= maxSize;
		ConcurrentStack<ByteBuf> queue = slabs[32 - numberOfLeadingZeros(buf.array.length - 1)];
		assert !queue.contains(buf) : "duplicate recycle array";
		queue.push(buf);
	}

	public static ByteBuf recycleIfEmpty(ByteBuf buf) {
		if (buf.canRead())
			return buf;
		buf.recycle();
		return ByteBuf.empty();
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

	private static ConcurrentStack<ByteBuf>[] createSlabs(int numberOfSlabs) {
		//noinspection unchecked
		ConcurrentStack<ByteBuf>[] slabs = new ConcurrentStack[numberOfSlabs];
		for (int i = 0; i < slabs.length; i++) {
			slabs[i] = new ConcurrentStack<>();
		}
		return slabs;
	}

	public static ByteBuf ensureTailRemaining(ByteBuf buf, int newTailRemaining) {
		assert !(buf instanceof ByteBuf.ByteBufSlice);
		if (buf.tailRemaining() >= newTailRemaining) {
			return buf;
		} else {
			ByteBuf newBuf = allocate(newTailRemaining + buf.headRemaining());
			newBuf.put(buf);
			buf.recycle();
			return newBuf;
		}
	}

	public static ByteBuf append(ByteBuf to, ByteBuf from) {
		assert !to.isRecycled() && !from.isRecycled();
		if (to.headRemaining() == 0) {
			to.recycle();
			return from;
		}
		to = ensureTailRemaining(to, from.headRemaining());
		to.put(from);
		from.recycle();
		return to;
	}

	public static ByteBuf append(ByteBuf to, byte[] from, int offset, int length) {
		assert !to.isRecycled();
		to = ensureTailRemaining(to, length);
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
		for (int n : created) {
			items += n;
		}
		return items;
	}

	public static int getCreatedItems(int slab) {
		assert slab >= 0 && slab < slabs.length;
		return created[slab];
	}

	public static int getPoolItems(int slab) {
		assert slab >= 0 && slab < slabs.length;
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
