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

public final class ScheduledRunnable implements Comparable<ScheduledRunnable>, AsyncCancellable {
	/**
	 * The time after which this runnable will be executed
	 */
	private final long timestamp;
	private Runnable runnable;
	private boolean cancelled;
	private boolean complete;

	// region builders

	/**
	 * Initializes a new instance of ScheduledRunnable
	 *
	 * @param timestamp timestamp after which this runnable will be executed
	 * @param runnable  runnable for executing
	 */
	private ScheduledRunnable(long timestamp, Runnable runnable) {
		this.timestamp = timestamp;
		this.runnable = runnable;
	}

	public static ScheduledRunnable create(long timestamp, Runnable runnable) {
		return new ScheduledRunnable(timestamp, runnable);
	}
	// endregion

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
		this.runnable = null;
	}

	public void complete() {
		this.complete = true;
		this.runnable = null;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public Runnable getRunnable() {
		return runnable;
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
				+ "complete=" + complete + ", "
				+ "runnable=" + runnable
				+ "}";
	}
}