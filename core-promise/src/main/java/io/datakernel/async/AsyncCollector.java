/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.async;

import io.datakernel.annotation.Nullable;
import io.datakernel.exception.UncheckedException;

import static io.datakernel.util.Preconditions.checkState;

public final class AsyncCollector<R> implements Cancellable {
	@FunctionalInterface
	public interface Accumulator<R, T> {
		void accumulate(R result, T value) throws UncheckedException;
	}

	@Nullable
	private final SettablePromise<R> resultPromise = new SettablePromise<>();
	private boolean running;

	private R result;

	private int activePromises;

	public AsyncCollector(R initialResult) {
		this.result = initialResult;
	}

	public static <R> AsyncCollector<R> create(R initialResult) {
		return new AsyncCollector<>(initialResult);
	}

	public <T> AsyncCollector<R> withPromise(Promise<T> promise, Accumulator<R, T> accumulator) {
		addPromise(promise, accumulator);
		return this;
	}

	public AsyncCollector<R> run() {
		checkState(!running);
		this.running = true;
		if (activePromises == 0) {
			resultPromise.set(result);
			result = null;
		}
		return this;
	}

	public AsyncCollector<R> run(Promise<Void> runtimePromise) {
		withPromise(runtimePromise, (result, v) -> {});
		return run();
	}

	public <T> Promise<T> addPromise(Promise<T> promise, Accumulator<R, T> accumulator) {
		checkState(!resultPromise.isComplete());
		activePromises++;
		return promise.whenComplete((v, e) -> {
			activePromises--;
			if (resultPromise.isComplete()) return;
			if (e == null) {
				try {
					accumulator.accumulate(result, v);
				} catch (UncheckedException u) {
					resultPromise.setException(u.getCause());
					result = null;
					return;
				}
				if (activePromises == 0 && running) {
					resultPromise.set(result);
				}
			} else {
				resultPromise.setException(e);
				result = null;
			}
		});
	}

	public <V> SettablePromise<V> newPromise(Accumulator<R, V> accumulator) {
		SettablePromise<V> resultPromise = new SettablePromise<>();
		addPromise(resultPromise, accumulator);
		return resultPromise;
	}

	public MaterializedPromise<R> get() {
		return resultPromise;
	}

	public int getActivePromises() {
		return activePromises;
	}

	@Override
	public void close(Throwable e) {
		checkState(!running);
		resultPromise.trySetException(e);
		result = null;
	}
}
