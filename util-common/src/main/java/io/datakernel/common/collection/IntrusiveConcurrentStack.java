/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.common.collection;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Concurrent implementation of stack data structure (Last In First Out)
 *
 * @param <E>
 */
public final class IntrusiveConcurrentStack<E extends IntrusiveConcurrentStack.Node> implements Iterable<E> {

	public interface Node {
		Node getNext();

		void setNext(Node next);
	}

	private final AtomicReference<Node> head = new AtomicReference<>();

	public void push(E newHead) {
		Node oldHead;
		do {
			oldHead = head.get();
			newHead.setNext(oldHead);
		} while (!head.compareAndSet(oldHead, newHead));
	}

	@SuppressWarnings("unchecked")
	public E pop() {
		Node oldHead;
		Node newHead;
		do {
			oldHead = head.get();
			if (oldHead == null) {
				return null;
			}
			newHead = oldHead.getNext();
		} while (!head.compareAndSet(oldHead, newHead));
		return (E) oldHead;
	}

	@SuppressWarnings("unchecked")
	public E peek() {
		Node node = head.get();
		return node == null ? null : (E) node;
	}

	public void clear() {
		head.set(null);
	}

	public boolean isEmpty() {
		return peek() == null;
	}

	public int size() {
		int result = 0;
		Node node = head.get();
		while (node != null) {
			node = node.getNext();
			result++;
		}
		return result;
	}

	@Override
	public Iterator<E> iterator() {
		return new Iterator<E>() {
			Node node = head.get();

			@Override
			public boolean hasNext() {
				return node != null;
			}

			@SuppressWarnings("unchecked")
			@Override
			public E next() {
				E result = (E) node;
				node = node.getNext();
				return result;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("You can not remove items from concurrent stack");
			}
		};
	}

}
