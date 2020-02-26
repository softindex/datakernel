package io.datakernel.bytebuf;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.lang.Integer.numberOfLeadingZeros;

/**
 * Optimized lock-free concurrent queue implementation for the {@link ByteBuf ByteBufs} that is used in {@link ByteBufPool}
 */
final class ByteBufConcurrentQueue {
	private final AtomicLong pos = new AtomicLong(0);
	private final AtomicReference<AtomicReferenceArray<ByteBuf>> array = new AtomicReference<>(new AtomicReferenceArray<>(1));
	private final ConcurrentHashMap<Integer, ByteBuf> map = new ConcurrentHashMap<>();

	final AtomicInteger realMin = new AtomicInteger(0);

	@Nullable
	public ByteBuf poll() {
		long pos1, pos2;
		int head, tail;
		do {
			pos1 = pos.get();
			head = (int) (pos1 >>> 32);
			tail = (int) pos1;
			if (head == tail) {
				return null;
			}
			tail++;
			if (ByteBufPool.USE_WATCHDOG) {
				int size = head - tail;
				realMin.updateAndGet(prevMin -> Math.min(prevMin, size));
			}
			pos2 = ((long) head << 32) + (tail & 0xFFFFFFFFL);
		} while (!pos.compareAndSet(pos1, pos2));

		Integer boxedTail = null;

		while (true) {
			AtomicReferenceArray<ByteBuf> bufs = array.get();
			ByteBuf buf = bufs.getAndSet(tail & (bufs.length() - 1), null);
			if (buf == null) {
				if (boxedTail == null) {
					boxedTail = tail;
				}
				buf = map.remove(boxedTail);
				if (buf == null) {
					Thread.yield();
					continue;
				}
			}
			if (buf.pos == tail) {
				return buf;
			}
			map.put(buf.pos, buf);
		}
	}

	public void offer(@NotNull ByteBuf buf) {
		long pos1, pos2;
		do {
			pos1 = pos.get();
			pos2 = pos1 + 0x100000000L;
		} while (!pos.compareAndSet(pos1, pos2));

		int head = (int) (pos2 >>> 32);
		buf.pos = head;

		AtomicReferenceArray<ByteBuf> bufs = array.get();
		int idx = head & (bufs.length() - 1);
		ByteBuf buf2 = bufs.getAndSet(idx, buf);
		if (buf2 == null && bufs == array.get()) {
			return; // fast path, everything is fine
		}
		// otherwise, evict bufs into map to make it retrievable by corresponding pop()
		pushToMap(bufs, idx, buf2);
	}

	private void pushToMap(AtomicReferenceArray<ByteBuf> bufs, int idx, @Nullable ByteBuf buf2) {
		ByteBuf buf3 = bufs.getAndSet(idx, null); // bufs may be stale at this moment, evict the data from this cell
		if (buf2 == null && buf3 == null) return;
		if (buf2 != null) map.put(buf2.pos, buf2);
		if (buf3 != null) map.put(buf3.pos, buf3);
		ensureCapacity(); // resize if needed
	}

	private void ensureCapacity() {
		int capacityNew = 1 << 32 - numberOfLeadingZeros(size() * 4 - 1);
		if (array.get().length() >= capacityNew) return;
		resize(capacityNew);
	}

	private void resize(int capacityNew) {
		AtomicReferenceArray<ByteBuf> bufsNew = new AtomicReferenceArray<>(capacityNew);
		AtomicReferenceArray<ByteBuf> bufsOld = array.getAndSet(bufsNew);
		// evict everything from old bufs array
		for (int i = 0; i < bufsOld.length(); i++) {
			ByteBuf buf = bufsOld.getAndSet(i, null);
			if (buf != null) map.put(buf.pos, buf);
		}
	}

	public List<ByteBuf> getBufs() {
		AtomicReferenceArray<ByteBuf> bufs = array.get();
		List<ByteBuf> list = new ArrayList<>();
		for (int i = 0; i < bufs.length(); i++) {
			ByteBuf buf = bufs.get(i);
			if (buf != null) {
				list.add(buf);
			}
		}
		return list;
	}

	public void clear() {
		while (!isEmpty()) {
			poll();
		}
	}

	public boolean isEmpty() {
		return size() == 0;
	}

	public int size() {
		long pos1 = pos.get();
		int head = (int) (pos1 >>> 32);
		int tail = (int) pos1;
		return head - tail;
	}

	@Override
	public String toString() {
		return "ByteBufConcurrentStack{" +
				"size=" + size() +
				", array=" + array.get().length() +
				", map=" + map.size() +
				'}';
	}
}
