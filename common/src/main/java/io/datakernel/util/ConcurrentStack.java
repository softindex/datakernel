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

package io.datakernel.util;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Concurrent implementation of stack data structure (Last In First Out)
 *
 * @param <E>
 */
public final class ConcurrentStack<E> implements Iterable<E> {
	public static final class Node<E> {
		private E item;
		private Node<E> next;
	}

	private final AtomicReference<Node<E>> cachedNodes;
	private final AtomicReference<Node<E>> head = new AtomicReference<>();

	public ConcurrentStack() {
		this.cachedNodes = new AtomicReference<>();
	}

	public ConcurrentStack(AtomicReference<Node<E>> cachedNodes) {
		this.cachedNodes = cachedNodes;
	}

	public void push(E item) {
		Node<E> newHead = popCachedNode();
		if (newHead == null) {
			newHead = new Node<>();
		}
		newHead.item = item;
		Node<E> oldHead;
		do {
			oldHead = head.get();
			newHead.next = oldHead;
		} while (!head.compareAndSet(oldHead, newHead));
	}

	private Node<E> popCachedNode() {
		Node<E> oldHead;
		Node<E> newHead;
		do {
			oldHead = cachedNodes.get();
			if (oldHead == null)
				return null;
			newHead = oldHead.next;
		} while (!cachedNodes.compareAndSet(oldHead, newHead));
		return oldHead;
	}

	public E pop() {
		Node<E> oldHead;
		Node<E> newHead;
		do {
			oldHead = head.get();
			if (oldHead == null)
				return null;
			newHead = oldHead.next;
		} while (!head.compareAndSet(oldHead, newHead));
		E result = oldHead.item;
		oldHead.item = null;
		pushCachedNode(oldHead);
		return result;
	}

	private void pushCachedNode(Node<E> node) {
		Node<E> oldHead;
		do {
			oldHead = cachedNodes.get();
			node.next = oldHead;
		} while (!cachedNodes.compareAndSet(oldHead, node));
	}

	public E peek() {
		Node<E> node = head.get();
		return node == null ? null : node.item;
	}

	public void clear() {
		head.set(null);
	}

	public boolean isEmpty() {
		return peek() == null;
	}

	public int size() {
		int result = 0;
		Node<E> node = head.get();
		while (node != null) {
			node = node.next;
			result++;
		}
		return result;
	}

	@Override
	public Iterator<E> iterator() {
		return new Iterator<E>() {
			Node<E> node = head.get();

			@Override
			public boolean hasNext() {
				return node != null;
			}

			@Override
			public E next() {
				E result = node.item;
				node = node.next;
				return result;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("You can not remove items from concurrent stack");
			}
		};
	}

}
