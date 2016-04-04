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

package io.datakernel.stream;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;

/**
 * Example 5.
 * Example of creating the custom StreamProducer, which passes the specified number to its data receiver each second.
 */
public class ScheduledProducer extends AbstractStreamProducer<Integer> {
	protected int numberToSend = 0;

	protected ScheduledRunnable scheduledRunnable;

	public ScheduledProducer(Eventloop eventloop) {
		super(eventloop);
	}

	@Override
	protected void onStarted() {
		scheduleNext();
	}

	@Override
	protected void onDataReceiverChanged() {

	}

	private void cancel() {
		if (scheduledRunnable != null) {
			scheduledRunnable.cancel();
			scheduledRunnable = null;
		}
	}

	public void abort() {
		cancel();
		sendEndOfStream();
	}

	public void scheduleNext() {
		if (scheduledRunnable != null && getProducerStatus().isClosed())
			return;
		scheduledRunnable = eventloop.schedule(eventloop.currentTimeMillis() + 1000L, new Runnable() {
			@Override
			public void run() {
				send(numberToSend++);
				scheduleNext();
			}
		});
	}

	@Override
	protected void onSuspended() {
		cancel();
	}

	@Override
	protected void onResumed() {
		scheduleNext();
	}

}
