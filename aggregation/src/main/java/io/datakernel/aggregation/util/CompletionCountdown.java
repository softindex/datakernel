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

package io.datakernel.aggregation.util;

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;

public final class CompletionCountdown {
	private final Eventloop eventloop;
	private final CompletionCallback callback;

	private int counter;
	private boolean done;

	private CompletionCountdown(Eventloop eventloop, CompletionCallback callback, int initialCounter) {
		this.eventloop = eventloop;
		this.callback = callback;
		this.counter = initialCounter;
	}

	private static CompletionCountdown create(Eventloop eventloop, CompletionCallback callback, int initialCounter) {
		return new CompletionCountdown(eventloop, callback, initialCounter);
	}

	public void increment() {
		counter++;
	}

	public void decrement() {
		if (--counter == 0 && !done) {
			done = true;
			callback.setComplete();
		}
	}

	public void postDecrement() {
		if (--counter == 0 && !done) {
			done = true;
			callback.postComplete(eventloop);
		}
	}

	public void setException(Exception e) {
		if (!done) {
			done = true;
			callback.setException(e);
		}
	}

	public void postException(Exception e) {
		if (!done) {
			done = true;
			callback.postException(eventloop, e);
		}
	}

	public void cancel() {
		done = true;
	}

	public boolean isDone() {
		return done;
	}
}
