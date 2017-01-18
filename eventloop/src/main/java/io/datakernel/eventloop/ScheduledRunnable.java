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

public abstract class ScheduledRunnable implements Runnable, AsyncCancellable {
	private long timestamp;
	private boolean cancelled;
	private boolean complete;

	ScheduledRunnable next;
	ScheduledRunnable prev;
	ScheduledRunnableQueue queue;

	public ScheduledRunnable() {
	}

	public final void reset() {
		assert queue == null && prev == null && next == null;

		timestamp = 0;
		cancelled = false;
		complete = false;
	}

	public abstract void run();

	public final void cancel() {
		this.cancelled = true;
		if (queue != null) {
			queue.remove(this);
		}
	}

	public final void complete() {
		this.complete = true;
	}

	public final boolean isCancelled() {
		return cancelled;
	}

	public final boolean isComplete() {
		return complete;
	}

	public final boolean isScheduledNow() {
		return queue != null;
	}

	final long getTimestamp() {
		return timestamp;
	}

	final void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName()
				+ "{"
				+ "timestamp=" + timestamp + ", "
				+ "cancelled=" + cancelled + ", "
				+ "complete=" + complete + ", "
				+ "}";
	}
}