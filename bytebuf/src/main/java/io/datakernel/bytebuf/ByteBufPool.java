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

import io.datakernel.util.ConcurrentStack;
import io.datakernel.util.MemSize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.datakernel.bytebuf.ByteBufRegistry.ByteBufMetaInfo;
import static io.datakernel.bytebuf.ByteBufRegistry.ByteBufWrapper;
import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Math.max;
import static java.util.Comparator.comparingLong;

public final class ByteBufPool {
	private static final int NUMBER_SLABS = 33;

	private static final AtomicReference<ConcurrentStack.Node<ByteBuf>> cachedNodes = new AtomicReference<>();

	private static final ConcurrentStack<ByteBuf>[] slabs;
	private static final AtomicInteger[] created;

	private static final Object EMPTY_VALUE = new Object();
	private static final ConcurrentHashMap<ByteBufWrapper, Object> byteBufs = new ConcurrentHashMap<>();

	static {
		//noinspection unchecked
		slabs = new ConcurrentStack[NUMBER_SLABS];
		//noinspection unchecked
		created = new AtomicInteger[NUMBER_SLABS];
		for (int i = 0; i < NUMBER_SLABS; i++) {
			slabs[i] = new ConcurrentStack<>(cachedNodes);
			created[i] = new AtomicInteger();
		}
	}

	private ByteBufPool() {
	}

	public static ByteBuf allocate(int size) {
		if (size == 0) return ByteBuf.empty();
		int index = 32 - numberOfLeadingZeros(size - 1); // index==32 for size==0
		ConcurrentStack<ByteBuf> stack = slabs[index];
		ByteBuf buf = stack.pop();
		if (buf != null) {
			buf.reset();
			assert byteBufs.remove(new ByteBufWrapper(buf)) != buf;
		} else {
			buf = ByteBuf.wrapForWriting(new byte[1 << index]);
			buf.refs++;
			assert (long) created[index].incrementAndGet() != Long.MAX_VALUE;
		}
		assert ByteBufRegistry.recordAllocate(buf);
		return buf;
	}

	public static ByteBuf allocate(MemSize size) {
		return allocate(size.toInt());
	}

	public static void recycle(ByteBuf buf) {
		if (buf.array.length == 0) return;
		ConcurrentStack<ByteBuf> stack = slabs[32 - numberOfLeadingZeros(buf.array.length - 1)];
		assert byteBufs.put(new ByteBufWrapper(buf), EMPTY_VALUE) == null : "duplicate recycle array";
		stack.push(buf);
		assert ByteBufRegistry.recordRecycle(buf);
	}

	public static ByteBuf recycleIfEmpty(ByteBuf buf) {
		if (buf.canRead())
			return buf;
		buf.recycle();
		return ByteBuf.empty();
	}

	static ConcurrentStack<ByteBuf> getSlab(int index) {
		return slabs[index];
	}

	public static void clear() {
		for (int i = 0; i < ByteBufPool.NUMBER_SLABS; i++) {
			slabs[i].clear();
			created[i].set(0);
		}
		byteBufs.clear();
	}

	public static ByteBuf ensureWriteRemaining(ByteBuf buf, int newWriteRemaining) {
		return ensureWriteRemaining(buf, 0, newWriteRemaining);
	}

	public static ByteBuf ensureWriteRemaining(ByteBuf buf, int minSize, int newWriteRemaining) {
		if (newWriteRemaining == 0) return buf;
		if (buf.writeRemaining() < newWriteRemaining || buf instanceof ByteBuf.ByteBufSlice) {
			ByteBuf newBuf = allocate(max(minSize, newWriteRemaining + buf.readRemaining()));
			newBuf.put(buf);
			buf.recycle();
			return newBuf;
		} else {
			return buf;
		}
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

	public interface ByteBufPoolStatsMXBean {

		int getCreatedItems();

		int getPoolItems();

		long getPoolItemAvgSize();

		long getPoolSizeKB();

		List<String> getPoolSlabs();

		List<ByteBufJmxInfo> getOldestByteBufs_Details();

		List<String> getOldestByteBufs_Summary();

		boolean getOldestByteBufs_settings_StoreStackTrace();

		void setOldestByteBufs_settings_StoreStackTrace(boolean flag);

		boolean getOldestByteBufs_settings_StoreByteBufs();

		void setOldestByteBufs_settings_StoreByteBufs(boolean flag);

		int getOldestByteBufs_settings_MaxBytesInContent();

		void setOldestByteBufs_settings_MaxBytesInContent(int bytes);

		int getOldestByteBufs_settings_MaxByteBufsToShow();

		void setOldestByteBufs_settings_MaxByteBufsToShow(int bufs);

		int getTotalActiveByteBufs();

		long getBufs_TotalAllocated();

		long getBufs_TotalRecycled();

		long getBufs_TotalNotRecycled();

		long getBytes_TotalAllocated();

		long getBytes_TotalRecycled();

		long getBytes_TotalNotRecycled();

		void clearRegistry();

		ByteBufDetailedJmxInfo fetchDetailedByteBufInfo(int indexInList, int start, int to);
	}

	public static final class ByteBufJmxInfo {
		private final long duration;
		private final List<String> stackTrace;
		private final int size;
		private final int readPosition;
		private final int writePosition;
		private final String content;

		public ByteBufJmxInfo(long duration, List<String> stackTrace, int size, int readPosition, int writePosition, String content) {
			this.duration = duration;
			this.stackTrace = stackTrace;
			this.size = size;
			this.readPosition = readPosition;
			this.writePosition = writePosition;
			this.content = content;
		}

		public String getDuration() {
			return formatDuration(duration);
		}

		public List<String> getStackTrace() {
			return stackTrace;
		}

		public int getSize() {
			return size;
		}

		public int getReadPosition() {
			return readPosition;
		}

		public int getWritePosition() {
			return writePosition;
		}

		public String getContent() {
			return content;
		}
	}

	public static final class ByteBufDetailedJmxInfo {
		private final long duration;
		private final List<String> stackTrace;
		private final int size;
		private final int readPosition;
		private final int writePosition;
		private final String content;
		private final int queriedFirstByteIndex;
		private final int queriedLastByteIndex;
		private final String queriedBytes;
		private final String queriedBytesHex;

		public ByteBufDetailedJmxInfo(long duration, List<String> stackTrace, int size,
		                              int readPosition, int writePosition, String content,
		                              int queriedFirstByteIndex, int queriedLastByteIndex,
		                              String queriedBytes, String queriedBytesHex) {
			this.duration = duration;
			this.stackTrace = stackTrace;
			this.size = size;
			this.readPosition = readPosition;
			this.writePosition = writePosition;
			this.content = content;
			this.queriedFirstByteIndex = queriedFirstByteIndex;
			this.queriedLastByteIndex = queriedLastByteIndex;
			this.queriedBytes = queriedBytes;
			this.queriedBytesHex = queriedBytesHex;
		}

		public long getDuration() {
			return duration;
		}

		public List<String> getStackTrace() {
			return stackTrace;
		}

		public int getSize() {
			return size;
		}

		public int getReadPosition() {
			return readPosition;
		}

		public int getWritePosition() {
			return writePosition;
		}

		public String getContent() {
			return content;
		}

		public int getQueriedFirstByteIndex() {
			return queriedFirstByteIndex;
		}

		public int getQueriedLastByteIndex() {
			return queriedLastByteIndex;
		}

		public String getQueriedBytes() {
			return queriedBytes;
		}

		public String getQueriedBytesHex() {
			return queriedBytesHex;
		}
	}

	public static final class ByteBufPoolStats implements ByteBufPoolStatsMXBean {
		private volatile int maxBytesInContent = 25;
		private volatile int maxBufsToShow = 100;

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

		@Override
		public List<ByteBufJmxInfo> getOldestByteBufs_Details() {
			Map<ByteBufWrapper, ByteBufMetaInfo> activeBufs = ByteBufRegistry.getActiveByteBufs();
			List<ByteBufJmxInfo> bufsInfo = new ArrayList<>();

			long currentTimestamp = System.currentTimeMillis();
			int maxBufsToShowCached = maxBufsToShow;
			for (ByteBufWrapper wrapper : activeBufs.keySet()) {
				if (bufsInfo.size() == maxBufsToShowCached) {
					break;
				}

				ByteBufMetaInfo byteBufMetaInfo = activeBufs.get(wrapper);
				if (byteBufMetaInfo == null) {
					continue;
				}

				ByteBuf buf = wrapper.getByteBuf();
				if (buf == null) {
					continue;
				}

				long duration = currentTimestamp - byteBufMetaInfo.getAllocationTimestamp();

				List<String> stackTraceLines = fetchStackTrace(byteBufMetaInfo);

				String content = extractContent(buf, maxBytesInContent);

				ByteBufJmxInfo byteBufJmxInfo = new ByteBufJmxInfo(duration, stackTraceLines,
						buf.limit(), buf.readPosition(), buf.writePosition(), content);
				bufsInfo.add(byteBufJmxInfo);
			}

			bufsInfo.sort(comparingLong(item -> -item.duration));

			return bufsInfo;
		}

		@Override
		public List<String> getOldestByteBufs_Summary() {
			List<ByteBufJmxInfo> detailedInfo = getOldestByteBufs_Details();
			List<String> summaryLines = new ArrayList<>();
			summaryLines.add("Duration       Content");
			for (ByteBufJmxInfo info : detailedInfo) {
				summaryLines.add(String.format("%s   %s", info.getDuration(), info.getContent()));
			}
			return summaryLines;
		}

		@Override
		public boolean getOldestByteBufs_settings_StoreStackTrace() {
			return ByteBufRegistry.getStoreStackTrace();
		}

		@Override
		public void setOldestByteBufs_settings_StoreStackTrace(boolean flag) {
			ByteBufRegistry.setStoreStackTrace(flag);
		}

		@Override
		public boolean getOldestByteBufs_settings_StoreByteBufs() {
			return ByteBufRegistry.getStoreByteBufs();
		}

		@Override
		public void setOldestByteBufs_settings_StoreByteBufs(boolean flag) {
			ByteBufRegistry.setStoreByteBufs(flag);
		}

		@Override
		public int getOldestByteBufs_settings_MaxBytesInContent() {
			return maxBytesInContent;
		}

		@Override
		public void setOldestByteBufs_settings_MaxBytesInContent(int bytesInContent) {
			if (bytesInContent < 0) {
				throw new IllegalArgumentException("argument must be non-negative");
			}
			this.maxBytesInContent = bytesInContent;
		}

		@Override
		public int getOldestByteBufs_settings_MaxByteBufsToShow() {
			return maxBufsToShow;
		}

		@Override
		public void setOldestByteBufs_settings_MaxByteBufsToShow(int bufs) {
			if (bufs < 0) {
				throw new IllegalArgumentException("argument must be non-negative");
			}
			this.maxBufsToShow = bufs;
		}

		@Override
		public int getTotalActiveByteBufs() {
			return ByteBufRegistry.getActiveByteBufs().size();
		}

		@Override
		public long getBufs_TotalAllocated() {
			return ByteBufRegistry.getTotalAllocatedBufs();
		}

		@Override
		public long getBufs_TotalRecycled() {
			return ByteBufRegistry.getTotalRecycledBufs();
		}

		@Override
		public long getBufs_TotalNotRecycled() {
			return ByteBufRegistry.getTotalAllocatedBufs() - ByteBufRegistry.getTotalRecycledBufs();
		}

		@Override
		public long getBytes_TotalAllocated() {
			return ByteBufRegistry.getTotalAllocatedBytes();
		}

		@Override
		public long getBytes_TotalRecycled() {
			return ByteBufRegistry.getTotalRecycledBytes();
		}

		@Override
		public long getBytes_TotalNotRecycled() {
			return ByteBufRegistry.getTotalAllocatedBytes() - ByteBufRegistry.getTotalRecycledBytes();
		}

		@Override
		public void clearRegistry() {
			ByteBufRegistry.clearRegistry();
		}

		@Override
		public ByteBufDetailedJmxInfo fetchDetailedByteBufInfo(int indexInList, int start, int to) {
			Map<ByteBufWrapper, ByteBufMetaInfo> activeBufs = ByteBufRegistry.getActiveByteBufs();
			List<ByteBufDetailedJmxInfo> bufsInfo = new ArrayList<>();

			long currentTimestamp = System.currentTimeMillis();
			int maxBufsToShowCached = maxBufsToShow;
			for (ByteBufWrapper wrapper : activeBufs.keySet()) {
				if (bufsInfo.size() == maxBufsToShowCached) {
					break;
				}

				ByteBufMetaInfo byteBufMetaInfo = activeBufs.get(wrapper);
				if (byteBufMetaInfo == null) {
					continue;
				}

				ByteBuf buf = wrapper.getByteBuf();
				if (buf == null) {
					continue;
				}

				long duration = currentTimestamp - byteBufMetaInfo.getAllocationTimestamp();

				List<String> stackTraceLines = fetchStackTrace(byteBufMetaInfo);

				String content = extractContent(buf, maxBytesInContent);

				byte[] queriedBytes = Arrays.copyOfRange(buf.array(), start, to);
				String queriedBytesStr = new String(queriedBytes);

				StringBuilder queriedBytesHex = new StringBuilder(queriedBytes.length * 3);
				for (byte queriedByte : queriedBytes) {
					queriedBytesHex.append(byteToHex(queriedByte));
					queriedBytesHex.append(" ");
				}

				ByteBufDetailedJmxInfo byteBufJmxInfo = new ByteBufDetailedJmxInfo(duration, stackTraceLines,
						buf.limit(), buf.readPosition(), buf.writePosition(), content,
						start, to, queriedBytesStr, queriedBytesHex.toString());
				bufsInfo.add(byteBufJmxInfo);
			}

			bufsInfo.sort(comparingLong(item -> -item.duration));

			return bufsInfo.get(indexInList);
		}

		private List<String> fetchStackTrace(ByteBufMetaInfo byteBufMetaInfo) {
			List<String> stackTraceLines = new ArrayList<>();
			StackTraceElement[] stackTrace = byteBufMetaInfo.getStackTrace();
			if (stackTrace != null) {
				for (StackTraceElement stackTraceElement : stackTrace) {
					stackTraceLines.add(stackTraceElement.toString());
				}
			}
			return stackTraceLines;
		}

		private String byteToHex(byte b) {
			return String.format("%02X", b);
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
