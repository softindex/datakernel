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

package io.datakernel.http;

import io.datakernel.annotation.Nullable;

public final class ExposedLinkedList<T> {
	private ExposedLinkedList() {}

	public static <T> ExposedLinkedList<T> create() {return new ExposedLinkedList<T>();}

	public static final class Node<T> {
		final T value;
		Node<T> prev;
		Node<T> next;

		public Node<T> getPrev() {
			return prev;
		}

		public Node<T> getNext() {
			return next;
		}

		public T getValue() {
			return value;
		}

		Node(T value) {
			this.value = value;
		}
	}

	private int size;
	private Node<T> first;
	private Node<T> last;

	public Node<T> getFirstNode() {
		return first;
	}

	public Node<T> getLastNode() {
		return last;
	}

	@Nullable
	public T getFirstValue() {
		return first != null ? first.value : null;
	}

	@Nullable
	public T getLastValue() {
		return last != null ? last.value : null;
	}

	public boolean isEmpty() {
		return first == null;
	}

	public void clear() {
		first = null;
		last = null;
	}

	public int size() {
		return size;
	}

	public Node<T> removeFirstNode() {
		if (first == null)
			return null;
		Node<T> node = first;
		first = node.next;
		if (node.next != null) {
			node.next.prev = node.prev;
		} else {
			assert last == node;
			last = node.prev;
		}
		size--;
		return node;
	}

	public Node<T> removeLastNode() {
		if (last == null)
			return null;
		Node<T> node = last;
		last = node.prev;
		if (node.prev != null) {
			node.prev.next = node.next;
		} else {
			assert first == node;
			first = node.next;
		}
		size--;
		return node;
	}

	@Nullable
	public T removeFirstValue() {
		Node<T> node = removeFirstNode();
		if (node == null) {
			return null;
		}
		return node.getValue();
	}

	@Nullable
	public T removeLastValue() {
		Node<T> node = removeLastNode();
		if (node == null) {
			return null;
		}
		return node.getValue();
	}

	public Node<T> addFirstValue(T value) {
		Node<T> node = new Node<>(value);
		addFirstNode(node);
		return node;
	}

	public Node<T> addLastValue(T value) {
		Node<T> node = new Node<>(value);
		addLastNode(node);
		return node;
	}

	public void addFirstNode(Node<T> node) {
		assert node.prev == null && node.next == null;
		if (first != null) {
			assert first.prev == null;
			first.prev = node;
			node.next = first;
		} else {
			assert last == null;
			last = node;
		}
		first = node;
		size++;
	}

	public void addLastNode(Node<T> node) {
		assert node.prev == null && node.next == null;
		if (last != null) {
			assert last.next == null;
			last.next = node;
			node.prev = last;
		} else {
			assert first == null;
			first = node;
		}
		last = node;
		size++;
	}

	public void moveNodeToLast(Node<T> node) {
		if (node.next == null)
			return;
		removeNode(node);
		addLastNode(node);
	}

	public void moveNodeToFirst(Node<T> node) {
		if (node.prev == null)
			return;
		removeNode(node);
		addFirstNode(node);
	}

	public void removeNode(Node<T> node) {
		if (node.prev != null) {
			node.prev.next = node.next;
		} else {
			assert first == node;
			first = node.next;
		}
		if (node.next != null) {
			node.next.prev = node.prev;
		} else {
			assert last == node;
			last = node.prev;
		}
		node.next = node.prev = null;
		size--;
	}

}
