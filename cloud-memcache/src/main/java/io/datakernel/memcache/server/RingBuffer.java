package io.datakernel.memcache.server;

import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.ObjectLongHashMap;
import io.datakernel.jmx.EventStats;
import io.datakernel.memcache.protocol.MemcacheRpcMessage.Slice;

import java.time.Duration;
import java.util.Arrays;

import static io.datakernel.jmx.MBeanFormat.formatTimestamp;
import static io.datakernel.util.StringFormatUtils.formatDuration;
import static java.lang.System.currentTimeMillis;

/**
 * The implementation to handle the big amount of date
 * It works like cache, when you use it you shouldn`t rely on result.
 * Because it can be rewritten by new date, if the written date was oversize
 */
public final class RingBuffer implements RingBufferMBean {

	/**
	 * The main class for the caching the byte-arrays
	 */
	private static class Buffer {
		private final byte[] array;
		private final IntLongHashMap indexInt = new IntLongHashMap();
		private final LongLongHashMap indexLong = new LongLongHashMap();
		private final ObjectLongHashMap<byte[]> indexBytes = new ObjectLongHashMap<byte[]>() {
			@Override
			protected int hashKey(byte[] key) {
				int result = 0;
				for (byte element : key) {
					result = 92821 * result + element;
				}
				return result;
			}

			@Override
			protected boolean equals(Object v1, Object v2) {
				return Arrays.equals((byte[]) v1, (byte[]) v2);
			}
		};

		private int position = 0;
		private long timestamp;

		Buffer(int capacity) {
			this.array = new byte[capacity];
			this.timestamp = currentTimeMillis();
		}

		void clear() {
			indexInt.clear();
			indexLong.clear();
			indexBytes.clear();
			position = 0;
			timestamp = currentTimeMillis();
		}

		int capacity() {
			return array.length;
		}

		int position() {
			return position;
		}

		static int intValueOf(byte[] bytes) {
			return ((bytes[0] << 24)) |
					((bytes[1] & 0xff) << 16) |
					((bytes[2] & 0xff) << 8) |
					((bytes[3] & 0xff));
		}

		static long longValueOf(byte[] bytes) {
			return ((((long) bytes[0]) << 56) |
					(((long) bytes[1] & 0xff) << 48) |
					(((long) bytes[2] & 0xff) << 40) |
					(((long) bytes[3] & 0xff) << 32) |
					(((long) bytes[4] & 0xff) << 24) |
					(((long) bytes[5] & 0xff) << 16) |
					(((long) bytes[6] & 0xff) << 8) |
					(((long) bytes[7] & 0xff)));
		}

		Slice get(byte[] key) {
			long segment;
			if (key.length == 4) {
				segment = indexInt.getOrDefault(intValueOf(key), -1L);
			} else if (key.length == 8) {
				segment = indexLong.getOrDefault(longValueOf(key), -1L);
			} else {
				segment = indexBytes.getOrDefault(key, -1L);
			}
			if (segment < 0)
				return null;
			int offset = (int) (segment);
			int size = (int) (segment >>> 32);
			return new Slice(array, offset, size);
		}

		void put(byte[] key, byte[] data, int offset, int length) {
			assert length <= remaining();
			long segment = ((long) length << 32) | position;
			if (key.length == 4) {
				indexInt.put(intValueOf(key), segment);
			} else if (key.length == 8) {
				indexLong.put(longValueOf(key), segment);
			} else {
				indexBytes.put(key, segment);
			}
			System.arraycopy(data, offset, array, position, length);
			position += length;
		}

		int remaining() {
			return array.length - position;
		}

		long getTimestamp() {
			return timestamp;
		}

		int items() {
			return indexInt.size() + indexLong.size() + indexBytes.size();
		}
	}

	private final Buffer[] ringBuffers;
	private int currentBuffer = 0;

	// JMX
	private static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);
	private final EventStats statsPuts = EventStats.create(SMOOTHING_WINDOW);
	private final EventStats statsGets = EventStats.create(SMOOTHING_WINDOW);
	private final EventStats statsMisses = EventStats.create(SMOOTHING_WINDOW);
	private int countCycles = 0;

	public static RingBuffer create(int amountBuffers, long bufferCapacity) {
		Buffer[] ringBuffers = new Buffer[amountBuffers];
		for (int i = 0; i < amountBuffers; i++) {
			ringBuffers[i] = new Buffer((int) bufferCapacity);
		}
		return new RingBuffer(ringBuffers);
	}

	private RingBuffer(Buffer[] ringBuffers) {
		this.ringBuffers = ringBuffers;
	}

	/**
	 * The method is used to try to get the from the {@see Buffer}
	 * It will return the latest actual data for the {@param key}
	 *
	 * @param key of your item
	 * @return the item in case your item is still present in {@see Buffer}
	 */
	public Slice get(byte[] key) {
		statsGets.recordEvent();
		for (int i = 0; i < ringBuffers.length; i++) {
			int current = currentBuffer - i;
			if (current < 0)
				current = ringBuffers.length + current;
			Slice slice = ringBuffers[current].get(key);
			if (slice != null) {
				return slice;
			}
		}
		statsMisses.recordEvent();
		return null;
	}

	/**
	 * The method is used to cache the actual information for the {@param key}
	 *
	 * @param key  is used as a pointer for the cached {@param data}
	 * @param data is thing to need to cache
	 */
	public void put(byte[] key, byte[] data) {
		put(key, data, 0, data.length);
	}

	/**
	 * The same to the above method,
	 * there are extra params to handle the {@param data}
	 */
	public void put(byte[] key, byte[] data, int offset, int length) {
		statsPuts.recordEvent();
		if (ringBuffers[currentBuffer].remaining() < length) {
			if (currentBuffer == ringBuffers.length - 1) {
				countCycles++;
			}
			currentBuffer = (currentBuffer + 1) % ringBuffers.length;
			ringBuffers[currentBuffer].clear();
		}
		ringBuffers[currentBuffer].put(key, data, offset, length);
	}

	private long getLifetimeMillis() {
		return currentTimeMillis() - ringBuffers[(currentBuffer + 1) % ringBuffers.length].getTimestamp();
	}

	// JMX
	@Override
	public void reset() {
		countCycles = 0;
		statsMisses.resetStats();
	}

	@Override
	public String getStatsPuts() {
		return statsPuts.toString();
	}

	@Override
	public double getStatsPutsRate() {
		return statsPuts.getSmoothedRate();
	}

	@Override
	public long getStatsPutsTotal() {
		return statsPuts.getTotalCount();
	}

	@Override
	public String getStatsGets() {
		return statsGets.toString();
	}

	@Override
	public double getStatsGetsRate() {
		return statsGets.getSmoothedRate();
	}

	@Override
	public long getStatsGetsTotal() {
		return statsGets.getTotalCount();
	}

	@Override
	public String getStatsMisses() {
		return statsMisses.toString();
	}

	@Override
	public double getStatsMissesRate() {
		return statsMisses.getSmoothedRate();
	}

	@Override
	public long getStatsMissesTotal() {
		return statsMisses.getTotalCount();
	}

	/**
	 * Is used to figure out the amount of byte[] arrays which are stored
	 *
	 * @return amount of stored data
	 */
	@Override
	public int getItems() {
		int items = 0;
		for (Buffer ringBuffer : ringBuffers) {
			items += ringBuffer.items();
		}
		return items;
	}

	/**
	 * Is used to get the occupied capacity
	 *
	 * @return amount of occupied capacity
	 */
	@Override
	public long getSize() {
		long size = 0;
		for (Buffer ringBuffer : ringBuffers) {
			size += ringBuffer.position;
		}
		return size;
	}

	@Override
	public String getLifetime() {
		return formatDuration(Duration.ofMillis(getLifetimeMillis()));
	}

	@Override
	public long getLifetimeSeconds() {
		return getLifetimeMillis() / 1000;
	}

	@Override
	public String getCurrentBuffer() {
		return (currentBuffer + 1) + " / " + ringBuffers.length + " @ " +
				formatTimestamp(ringBuffers[currentBuffer].getTimestamp());
	}

	@Override
	public int getFullCycles() {
		return countCycles;
	}
}
