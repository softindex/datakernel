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

final class ConnectionsLinkedList {
	@Nullable
	private AbstractHttpConnection first;
	@Nullable
	private AbstractHttpConnection last;

	public boolean isEmpty() {
		return first == null;
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
		return closeExpiredConnections(expiration, null);
	}

	public int closeExpiredConnections(long expiration, @Nullable Exception e) {
		int count = 0;
		AbstractHttpConnection connection = first;
		while (connection != null) {
			AbstractHttpConnection next = connection.next;
			if (connection.poolTimestamp > expiration)
				break; // connections must back ordered by activity
			if (e == null)
				connection.close();
			else
				connection.closeWithError(e);
			assert connection.prev == null && connection.next == null;
			connection = next;
			count++;
		}
		return count;
	}

	public int closeAllConnections() {
		int count = 0;
		AbstractHttpConnection connection = first;
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
