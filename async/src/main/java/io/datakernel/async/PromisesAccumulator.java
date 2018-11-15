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

public final class PromisesAccumulator<A> {
	@Nullable
	private SettablePromise<A> resultPromise;

	private A accumulator;
	private Throwable exception;

	private int activePromises;

	private PromisesAccumulator(A initialAccumulator) {
		this.accumulator = initialAccumulator;
	}

	public static <A> PromisesAccumulator<A> create(A initialAccumulator) {
		return new PromisesAccumulator<>(initialAccumulator);
	}

	public <V> PromisesAccumulator<A> withPromise(Promise<V> promise, Reducer<A, V> reducer) {
		addPromise(promise, reducer);
		return this;
	}

	public <V> Promise<V> addPromise(Promise<V> promise, Reducer<A, V> reducer) {
		activePromises++;
		return promise.whenComplete((result, throwable) -> {
			if (throwable == null) {
				if (accumulator != null) {
					reducer.accumulate(accumulator, result);
					assert accumulator != null;
				}

				activePromises--;
				if (activePromises == 0 && resultPromise != null && exception == null) {
					resultPromise.set(accumulator);
					resultPromise = null;
				}
			} else {
				activePromises--;
				if (exception == null) {
					exception = throwable;
					//noinspection AssignmentToNull - resource release
					accumulator = null;
					if (resultPromise != null) {
						resultPromise.setException(throwable);
						resultPromise = null;
					}
				}
			}
		});
	}

	public <V> SettablePromise<V> newPromise(Reducer<A, V> reducer) {
		SettablePromise<V> resultPromise = new SettablePromise<>();
		addPromise(resultPromise, reducer);
		return resultPromise;
	}

	public Promise<A> get() {
		if (resultPromise != null)
			return resultPromise;
		resultPromise = new SettablePromise<>();
		if (exception != null) {
			resultPromise.setException(exception);
			return resultPromise;
		}
		if (activePromises == 0) {
			resultPromise.set(accumulator);
			return resultPromise;
		}
		return resultPromise;
	}

	public A getAccumulator() {
		return accumulator;
	}

	public Throwable getException() {
		return exception;
	}

	public int getActivePromises() {
		return activePromises;
	}
}
