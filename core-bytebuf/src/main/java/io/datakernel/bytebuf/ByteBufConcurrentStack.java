package io.datakernel.bytebuf;

import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * Simple lock-free concurrent stack implementation for the {@link ByteBuf ByteBufs} that is used in {@link ByteBufPool}
 */
final class ByteBufConcurrentStack {
	private final AtomicStampedReference<ByteBuf> head = new AtomicStampedReference<>(null, 0);

	public void push(ByteBuf newHead) {
		ByteBuf oldHead;
		int oldStamp;
		do {
			int[] oldStampRef = new int[1];
			oldHead = head.get(oldStampRef);
			oldStamp = oldStampRef[0];
			newHead.next = oldHead;
		} while (!head.compareAndSet(oldHead, newHead, oldStamp, oldStamp + 1));
	}

	public ByteBuf pop() {
		ByteBuf oldHead;
		ByteBuf newHead;
		int oldStamp;
		do {
			int[] oldStampRef = new int[1];
			oldHead = head.get(oldStampRef);
			oldStamp = oldStampRef[0];
			if (oldHead == null) {
				return null;
			}
			newHead = oldHead.next;
		} while (!head.compareAndSet(oldHead, newHead, oldStamp, oldStamp + 1));
		return oldHead;
	}

	public ByteBuf peek() {
		return head.getReference();
	}

	public void clear() {
		head.set(null, 0);
	}

	public boolean isEmpty() {
		return head.getReference() == null;
	}

	public int size() {
		int result = 0;
		ByteBuf node = head.getReference();
		while (node != null) {
			node = node.next;
			result++;
		}
		return result;
	}
}
