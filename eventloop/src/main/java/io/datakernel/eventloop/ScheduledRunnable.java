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

import io.datakernel.async.AsyncCancellable;

public abstract class ScheduledRunnable implements Comparable<ScheduledRunnable>, AsyncCancellable, Runnable {
	/**
	 * The time after which this runnable will be executed
	 */
	private long timestamp = 0L;
	private boolean cancelled;
	private boolean complete;

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ScheduledRunnable that = (ScheduledRunnable) o;
		return timestamp == that.timestamp;
	}

	@Override
	public int hashCode() {
		return (int) (timestamp ^ (timestamp >>> 32));
	}

	/**
	 * Compares timestamps of two ScheduledRunnables
	 *
	 * @param o ScheduledRunnable for comparing
	 * @return a negative integer, zero, or a positive integer as this
	 * timestamp is less than, equal to, or greater than the timestamp of
	 * ScheduledRunnable from argument.
	 */
	@Override
	public int compareTo(ScheduledRunnable o) {
		return Long.compare(timestamp, o.timestamp);
	}

	@Override
	public void cancel() {
		this.cancelled = true;
	}

	public void complete() {
		this.complete = true;
	}

	public long getTimestamp() {
		return timestamp;
	}

	void setTimestamp(long timestamp) {
		assert timestamp > 0L : "timestamp must be greater than zero";
		assert this.timestamp == 0L : "ScheduledRunnable cannot be reused";

		this.timestamp = timestamp;
	}

	public boolean isCancelled() {
		return cancelled;
	}

	public boolean isComplete() {
		return complete;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName()
				+ "{"
				+ "timestamp=" + timestamp + ", "
				+ "cancelled=" + cancelled + ", "
				+ "complete=" + complete
				+ "}";
	}
}