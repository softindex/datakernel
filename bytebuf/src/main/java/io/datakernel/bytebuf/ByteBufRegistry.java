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

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class ByteBufRegistry {
	public static final int CLEAR_EMPTY_WRAPPERS_PERIOD = 1000;

	private static final ConcurrentHashMap<ByteBufWrapper, ByteBufMetaInfo> activeByteBufs = new ConcurrentHashMap<>();
	private static AtomicInteger counter = new AtomicInteger(0);

	private static volatile boolean storeByteBufs = false;
	private static volatile boolean storeStackTrace = false;

	private static final AtomicLong totalAllocatedBufs = new AtomicLong(0);
	private static final AtomicLong totalRecycledBufs = new AtomicLong(0);

	private static final AtomicLong totalAllocatedBytes = new AtomicLong(0);
	private static final AtomicLong totalRecycledBytes = new AtomicLong(0);

	private ByteBufRegistry() {}

	// region public api
	public static void clearRegistry() {
		activeByteBufs.clear();
	}

	public static Map<ByteBufWrapper, ByteBufMetaInfo> getActiveByteBufs() {
		return activeByteBufs;
	}

	public static boolean getStoreStackTrace() {
		return storeStackTrace;
	}

	public static void setStoreStackTrace(boolean store) {
		storeStackTrace = store;
	}

	public static boolean getStoreByteBufs() {
		return storeByteBufs;
	}

	public static void setStoreByteBufs(boolean store) {
		storeByteBufs = store;
	}

	public static long getTotalAllocatedBufs() {
		return totalAllocatedBufs.longValue();
	}

	public static long getTotalRecycledBufs() {
		return totalRecycledBufs.longValue();
	}

	public static long getTotalAllocatedBytes() {
		return totalAllocatedBytes.longValue();
	}

	public static long getTotalRecycledBytes() {
		return totalRecycledBytes.longValue();
	}
	// endregion

	public static boolean recordAllocate(ByteBuf buf) {
		totalAllocatedBufs.incrementAndGet();
		totalAllocatedBytes.addAndGet(buf.array().length);
		if (storeByteBufs) {
			int current = counter.incrementAndGet();
			if (current % CLEAR_EMPTY_WRAPPERS_PERIOD == 0) { // in case of negative values it also works properly
				clearEmptyWrappers();
			}

			StackTraceElement[] stackTrace = null;
			if (storeStackTrace) {
				// TODO(vmykhalko): maybe use new Exception().getStackTrace instead ? according to performance issues
				StackTraceElement[] fullStackTrace = Thread.currentThread().getStackTrace();
				// remove stack trace lines that stand for registration method calls
				stackTrace = Arrays.copyOfRange(fullStackTrace, 3, fullStackTrace.length);
			}
			long timestamp = System.currentTimeMillis();
			ByteBufMetaInfo metaInfo = new ByteBufMetaInfo(stackTrace, timestamp);
			activeByteBufs.put(new ByteBufWrapper(buf), metaInfo);
		}

		return true;
	}

	public static boolean recordRecycle(ByteBuf buf) {
		totalRecycledBufs.incrementAndGet();
		totalRecycledBytes.addAndGet(buf.array().length);
		if (storeByteBufs) {
			activeByteBufs.remove(new ByteBufWrapper(buf));
		}

		return true;
	}

	private static void clearEmptyWrappers() {
		Iterator<Map.Entry<ByteBufWrapper, ByteBufMetaInfo>> iterator = activeByteBufs.entrySet().iterator();
		while (iterator.hasNext()) {
			ByteBufWrapper wrapper = iterator.next().getKey();
			if (wrapper.getByteBuf() == null) {
				iterator.remove();
			}
		}
	}

	static final class ByteBufWrapper {
		private final Reference<ByteBuf> bufRef;

		public ByteBufWrapper(ByteBuf buf) {
			this.bufRef = new SoftReference<>(buf);
		}

		public ByteBuf getByteBuf() {
			return bufRef.get();
		}

		@Override
		public boolean equals(Object o) {
			if (o == null) {
				return false;
			}

			if (o.getClass() != ByteBufWrapper.class) {
				return false;
			}

			ByteBufWrapper other = (ByteBufWrapper) o;
			return this.bufRef.get() == other.bufRef.get();
		}

		@Override
		public int hashCode() {
			return System.identityHashCode(bufRef.get());
		}
	}

	static final class ByteBufMetaInfo {
		private final StackTraceElement[] stackTrace;
		private final long allocateTimestamp;

		public ByteBufMetaInfo(StackTraceElement[] stackTrace, long allocateTimestamp) {
			this.stackTrace = stackTrace;
			this.allocateTimestamp = allocateTimestamp;
		}

		public StackTraceElement[] getStackTrace() {
			return stackTrace;
		}

		public long getAllocationTimestamp() {
			return allocateTimestamp;
		}
	}
}
