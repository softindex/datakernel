package io.datakernel.bytebuf;

import java.util.concurrent.atomic.AtomicReference;

final class ByteBufConcurrentStack {
	private final AtomicReference<ByteBuf> head = new AtomicReference<>();

	void push(ByteBuf newHead) {
		ByteBuf oldHead;
		do {
			oldHead = head.get();
			newHead.next = oldHead;
		} while (!head.compareAndSet(oldHead, newHead));
	}

	ByteBuf pop() {
		ByteBuf oldHead;
		ByteBuf newHead;
		do {
			oldHead = head.get();
			if (oldHead == null) {
				return null;
			}
			newHead = oldHead.next;
		} while (!head.compareAndSet(oldHead, newHead));
		return oldHead;
	}

	public ByteBuf peek() {
		return head.get();
	}

	void clear() {
		head.set(null);
	}

	boolean isEmpty() {
		return head.get() == null;
	}

	public int size() {
		int result = 0;
		ByteBuf node = head.get();
		while (node != null) {
			node = node.next;
			result++;
		}
		return result;
	}
}
