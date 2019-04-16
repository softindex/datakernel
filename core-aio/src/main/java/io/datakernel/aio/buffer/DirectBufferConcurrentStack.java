package io.datakernel.aio.buffer;

import java.util.concurrent.atomic.AtomicReference;

final class DirectBufferConcurrentStack {
	private final AtomicReference<LinkedDirectBuffer> head = new AtomicReference<>();

	void push(LinkedDirectBuffer newHead) {
		LinkedDirectBuffer oldHead;
		do {
			oldHead = head.get();
			newHead.next = oldHead;
		} while (!head.compareAndSet(oldHead, newHead));
	}

	LinkedDirectBuffer pop() {
		LinkedDirectBuffer oldHead;
		LinkedDirectBuffer newHead;
		do {
			oldHead = head.get();
			if (oldHead == null) {
				return null;
			}
			newHead = oldHead.next;
		} while (!head.compareAndSet(oldHead, newHead));

		return oldHead;
	}

	public LinkedDirectBuffer peek() {
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
		LinkedDirectBuffer node = head.get();
		while (node != null) {
			node = node.next;
			result++;
		}
		return result;
	}
}
