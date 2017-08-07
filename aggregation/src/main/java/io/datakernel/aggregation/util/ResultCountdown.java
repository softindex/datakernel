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

import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;

public final class ResultCountdown<A> {
	private final Eventloop eventloop;
	private final ResultCallback<A> callback;

	private A accumulator;

	private int counter;
	private boolean done;

	private ResultCountdown(Eventloop eventloop, ResultCallback<A> callback, A accumulator, int initialCounter) {
		this.eventloop = eventloop;
		this.callback = callback;
		this.accumulator = accumulator;
		this.counter = initialCounter;
	}

	public static <A> ResultCountdown<A> create(Eventloop eventloop, ResultCallback<A> callback, A result, int initialCounter) {
		return new ResultCountdown<>(eventloop, callback, result, initialCounter);
	}

	public A getAccumulator() {
		return accumulator;
	}

	public void setAccumulator(A result) {
		this.accumulator = result;
	}

	public int getCounter() {
		return counter;
	}

	public int increment() {
		return ++counter;
	}

	public int decrement() {
		if (--counter == 0 && !done) {
			done = true;
			callback.setResult(accumulator);
		}
		return counter;
	}

	public int postDecrement() {
		if (--counter == 0 && !done) {
			done = true;
			callback.postResult(eventloop, accumulator);
		}
		return counter;
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
