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

package io.datakernel.async.process;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.RetryPolicy;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import static io.datakernel.common.Preconditions.checkArgument;
import static io.datakernel.eventloop.RunnableWithContext.wrapContext;

public class AsyncExecutors {

	public static AsyncExecutor direct() {
		return new AsyncExecutor() {
			@NotNull
			@Override
			public <T> Promise<T> execute(@NotNull AsyncSupplier<T> supplier) {
				return supplier.get();
			}
		};
	}

	public static AsyncExecutor ofEventloop(@NotNull Eventloop eventloop) {
		return new AsyncExecutor() {
			@NotNull
			@Override
			public <T> Promise<T> execute(@NotNull AsyncSupplier<T> supplier) {
				Eventloop currentEventloop = Eventloop.getCurrentEventloop();
				if (eventloop == currentEventloop) {
					return supplier.get();
				}
				return Promise.ofCallback(cb -> {
					currentEventloop.startExternalTask();
					eventloop.execute(wrapContext(cb, () -> supplier.get()
							.whenComplete((result, e) -> {
								currentEventloop.execute(wrapContext(cb, () -> cb.accept(result, e)));
								currentEventloop.completeExternalTask();
							})));
				});
			}
		};
	}

	public static AsyncExecutor roundRobin(@NotNull List<AsyncExecutor> executors) {
		return new AsyncExecutor() {
			int index;

			@NotNull
			@Override
			public <T> Promise<T> execute(@NotNull AsyncSupplier<T> supplier) {
				AsyncExecutor executor = executors.get(index);
				index = (index + 1) % executors.size();
				return executor.execute(supplier);
			}
		};
	}

	public static AsyncExecutor sequential() {
		return buffered(1, Integer.MAX_VALUE);
	}

	public static AsyncExecutor buffered(int maxParallelCalls) {
		return buffered(maxParallelCalls, Integer.MAX_VALUE);
	}

	public static AsyncExecutor buffered(int maxParallelCalls, int maxBufferedCalls) {
		return new AsyncExecutor() {
			private int pendingCalls;
			private final ArrayDeque<Object> deque = new ArrayDeque<>();

			@SuppressWarnings({"unchecked", "ConstantConditions"})
			private void processBuffer() {
				while (pendingCalls < maxParallelCalls && !deque.isEmpty()) {
					AsyncSupplier<Object> supplier = (AsyncSupplier<Object>) deque.pollFirst();
					SettablePromise<Object> cb = (SettablePromise<Object>) deque.pollFirst();
					pendingCalls++;
					supplier.get()
							.whenComplete((result, e) -> {
								pendingCalls--;
								processBuffer();
								cb.accept(result, e);
							});
				}
			}

			@NotNull
			@Override
			public <T> Promise<T> execute(@NotNull AsyncSupplier<T> supplier) throws RejectedExecutionException {
				if (pendingCalls < maxParallelCalls) {
					pendingCalls++;
					return supplier.get().whenComplete(() -> {
						pendingCalls--;
						processBuffer();
					});
				}
				if (deque.size() > maxBufferedCalls) {
					throw new RejectedExecutionException("Too many operations running");
				}
				SettablePromise<T> result = new SettablePromise<>();
				deque.addLast(supplier);
				deque.addLast(result);
				return result;
			}
		};
	}

	public static AsyncExecutor retry(@NotNull RetryPolicy retryPolicy) {
		return new AsyncExecutor() {
			@NotNull
			@Override
			public <T> Promise<T> execute(@NotNull AsyncSupplier<T> supplier) {
				return Promises.retry(supplier, retryPolicy);
			}
		};
	}

	public static AsyncExecutor ofMaxRecursiveCalls(int maxRecursiveCalls) {
		checkArgument(maxRecursiveCalls >= 0, "Number of recursive calls cannot be less than 0");
		return new AsyncExecutor() {
			private final int maxCalls = maxRecursiveCalls + 1;
			private int counter = 0;

			@NotNull
			@Override
			public <T> Promise<T> execute(@NotNull AsyncSupplier<T> supplier) {
				Promise<T> promise = supplier.get();
				if (promise.isComplete() && counter++ % maxCalls == 0) {
					counter = 0;
					return promise.async();
				}
				return promise;
			}
		};
	}

}
