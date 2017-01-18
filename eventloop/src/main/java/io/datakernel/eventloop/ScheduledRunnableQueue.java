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

	public void add(ScheduledRunnable sr) {
		assert sr.prev == null && sr.next == null && sr.queue == null;

		long timestamp = sr.getTimestamp();
		ScheduledRunnable first = timestampToBucket.get(timestamp);
		if (first == null) {
			timestampToBucket.put(timestamp, sr);
		} else {
			assert timestamp == first.getTimestamp();

			sr.next = first;
			first.prev = sr;
			timestampToBucket.put(timestamp, sr);
		}
		sr.queue = this;
	}

	public ScheduledRunnable peek() {
		if (timestampToBucket.isEmpty()) {
			return null;
		}
		return timestampToBucket.get(timestampToBucket.firstKey());
	}

	public ScheduledRunnable poll() {
		if (timestampToBucket.isEmpty()) {
			return null;
		}

		long minTimestamp = timestampToBucket.firstKey();
		ScheduledRunnable first = timestampToBucket.get(minTimestamp);
		if (first.next == null) {
			timestampToBucket.remove(minTimestamp);
		} else {
			first.next.prev = null;
			timestampToBucket.put(minTimestamp, first.next);
			first.next = null;
		}

		return first;
	}

	public boolean isEmpty() {
		return timestampToBucket.isEmpty();
	}

	public int size() {
		int total = 0;
		for (ScheduledRunnable scheduledRunnable : timestampToBucket.values()) {
			ScheduledRunnable current = scheduledRunnable;
			while (current != null) {
				total++;
				current = current.next;
			}
		}
		return total;
	}

	void remove(ScheduledRunnable sr) {
		if (sr.prev == null) {
			if (sr.next == null) {
				timestampToBucket.remove(sr.getTimestamp());
			} else {
				sr.next.prev = null;
				timestampToBucket.put(sr.getTimestamp(), sr.next);
			}
		} else {
			sr.prev.next = sr.next;
			if (sr.next != null) {
				sr.next.prev = sr.prev;
			}
		}

		sr.prev = null;
		sr.next = null;
		sr.queue = null;
	}
}
