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

public final class ConnectionsLinkedList {
	private ConnectionsLinkedList() {
	}

	public static ConnectionsLinkedList create() {
		return new ConnectionsLinkedList();
	}

	private AbstractHttpConnection first;
	private AbstractHttpConnection last;

	public AbstractHttpConnection getFirstNode() {
		return first;
	}

	public AbstractHttpConnection getLastNode() {
		return last;
	}

	public boolean isEmpty() {
		return first == null;
	}

	public void clear() {
		first = null;
		last = null;
	}

	public AbstractHttpConnection removeFirstNode() {
		if (first == null)
			return null;
		AbstractHttpConnection node = first;
		first = node.next;
		if (node.next != null) {
			node.next.prev = node.prev;
		} else {
			assert last == node;
			last = node.prev;
		}
		node.next = node.prev = null;
		return node;
	}

	public AbstractHttpConnection removeLastNode() {
		if (last == null)
			return null;
		AbstractHttpConnection node = last;
		last = node.prev;
		if (node.prev != null) {
			node.prev.next = node.next;
		} else {
			assert first == node;
			first = node.next;
		}
		node.next = node.prev = null;
		return node;
	}

	public void addFirstNode(AbstractHttpConnection node) {
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
	}

	public void addLastNode(AbstractHttpConnection node) {
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
	}

	public void moveNodeToLast(AbstractHttpConnection node) {
		if (node.next == null)
			return;
		removeNode(node);
		addLastNode(node);
	}

	public void moveNodeToFirst(AbstractHttpConnection node) {
		if (node.prev == null)
			return;
		removeNode(node);
		addFirstNode(node);
	}

	public void removeNode(AbstractHttpConnection node) {
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
	}

	public int closeExpiredConnections(long expiration) {
		int count = 0;
		AbstractHttpConnection connection = getFirstNode();
		while (connection != null) {
			AbstractHttpConnection next = connection.next;
			if (connection.poolTimestamp < expiration)
				break; // connections must back ordered by activity
			connection.close();
			assert connection.prev == null && connection.next == null;
			connection = next;
			count++;
		}
		return count;
	}

	public int closeAllConnections() {
		int count = 0;
		AbstractHttpConnection connection = getFirstNode();
		while (connection != null) {
			AbstractHttpConnection next = connection.next;
			connection.close();
			assert connection.prev == null && connection.next == null;
			connection = next;
			count++;
		}
		return count;
	}

	public int size() {
		int count = 0;
		for (AbstractHttpConnection connection = first; connection != null; connection = connection.next) {
			count++;
		}
		return count;
	}
}
