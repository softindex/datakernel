package io.datakernel.bytebuf;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Simple lock-free concurrent stack implementation for the {@link ByteBuf ByteBufs} that is used in {@link ByteBufPool}
 */
final class ByteBufConcurrentStack {
	private static final class ByteBufRef {
		ByteBuf buf;
	}

	private final AtomicReference<ByteBufRef> head = new AtomicReference<>(new ByteBufRef());

	public void push(ByteBuf newHead) {
		ByteBufRef oldHeadRef;
		ByteBufRef newHeadRef = new ByteBufRef();
		newHeadRef.buf = newHead;
		do {
			oldHeadRef = head.get();
			newHead.next = oldHeadRef.buf;
		} while (!head.compareAndSet(oldHeadRef, newHeadRef));
	}

	public ByteBuf pop() {
		ByteBufRef oldHeadRef;
		ByteBufRef newHeadRef = new ByteBufRef();
		ByteBuf oldHead;
		do {
			oldHeadRef = head.get();
			oldHead = oldHeadRef.buf;
			if (oldHead == null) {
				return null;
			}
			newHeadRef.buf = oldHead.next;
		} while (!head.compareAndSet(oldHeadRef, newHeadRef));
		return oldHead;
	}

	public ByteBuf peek() {
		return head.get().buf;
	}

	public void clear() {
		head.set(new ByteBufRef());
	}

	public boolean isEmpty() {
		return head.get().buf == null;
	}

	public int size() {
		int result = 0;
		ByteBuf node = head.get().buf;
		while (node != null) {
			node = node.next;
			result++;
		}
		return result;
	}
}
