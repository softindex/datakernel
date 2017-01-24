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

package io.datakernel.eventloop;

import java.util.SortedMap;
import java.util.TreeMap;

final class ScheduledRunnableQueue {
	private final SortedMap<Long, ScheduledRunnable> timestampToBucket = new TreeMap<>();
	private ScheduledRunnable cachedFirst;

	public void add(ScheduledRunnable sr) {
		assert sr.prev == null && sr.next == null && sr.queue == null;

		sr.queue = this;

		if (cachedFirst == null) {
			cachedFirst = sr;
			return;
		}

		long timestamp = sr.getTimestamp();

		if (timestamp < cachedFirst.getTimestamp()) {
			timestampToBucket.put(cachedFirst.getTimestamp(), cachedFirst);
			cachedFirst = sr;
		} else if (timestamp == cachedFirst.getTimestamp()) {
			cachedFirst.prev = sr;
			sr.next = cachedFirst;
			cachedFirst = sr;
		} else {
			ScheduledRunnable firstInProperBucket = timestampToBucket.get(timestamp);
			if (firstInProperBucket == null) {
				timestampToBucket.put(timestamp, sr);
			} else {
				assert timestamp == firstInProperBucket.getTimestamp();

				sr.next = firstInProperBucket;
				firstInProperBucket.prev = sr;
				timestampToBucket.put(timestamp, sr);
			}
		}
	}

	public ScheduledRunnable peek() {
		return cachedFirst;
	}

	public ScheduledRunnable poll() {
		if (cachedFirst == null) {
			return null;
		}

		ScheduledRunnable first = cachedFirst;

		if (first.next != null) {
			first.next.prev = null;
			cachedFirst = first.next;
			first.next = null;
		} else {
			cacheFirstMapValue();
		}

		first.queue = null;
		return first;
	}

	private void cacheFirstMapValue() {
		if (!timestampToBucket.isEmpty()) {
			cachedFirst = timestampToBucket.remove(timestampToBucket.firstKey());
		} else {
			cachedFirst = null;
		}
	}

	public boolean isEmpty() {
		return cachedFirst == null;
	}

	public int size() {
		int total = 0;

		ScheduledRunnable current = cachedFirst;
		while (current != null) {
			total++;
			current = current.next;
		}

		for (ScheduledRunnable scheduledRunnable : timestampToBucket.values()) {
			current = scheduledRunnable;
			while (current != null) {
				total++;
				current = current.next;
			}
		}

		return total;
	}

	void remove(ScheduledRunnable sr) {
		assert cachedFirst != null;

		if (cachedFirst.getTimestamp() == sr.getTimestamp()) { // "sr" is located in cached bucket
			if (sr.prev == null) {
				if (sr.next == null) {
					assert sr == cachedFirst;

					cacheFirstMapValue();
				} else {
					sr.next.prev = null;
					cachedFirst = sr.next;
				}
			} else {
				removeNonFirstNode(sr);
			}
		} else { // "sr" is located in map
			if (sr.prev == null) {
				if (sr.next == null) {
					timestampToBucket.remove(sr.getTimestamp());
				} else {
					sr.next.prev = null;
					timestampToBucket.put(sr.getTimestamp(), sr.next);
				}
			} else {
				removeNonFirstNode(sr);
			}
		}

		sr.prev = null;
		sr.next = null;
		sr.queue = null;
	}

	private void removeNonFirstNode(ScheduledRunnable sr) {
		sr.prev.next = sr.next;
		if (sr.next != null) {
			sr.next.prev = sr.prev;
		}
	}
}
