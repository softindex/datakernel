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

import io.datakernel.eventloop.Eventloop;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import static io.datakernel.util.Preconditions.checkArgument;

public class AsyncExecutors {
	private AsyncExecutors() {}

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
					eventloop.execute(() -> supplier.get()
							.whenComplete((result, e) -> {
								currentEventloop.execute(() ->
										cb.set(result, e));
								currentEventloop.completeExternalTask();
							}));
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
					SettableCallback<Object> cb = (SettableCallback<Object>) deque.pollFirst();
					pendingCalls++;
					supplier.get().whenComplete((result, e) -> {
						pendingCalls--;
						processBuffer();
						cb.set(result, e);
					});
				}
			}

			@NotNull
			@Override
			public <T> Promise<T> execute(@NotNull AsyncSupplier<T> supplier) throws RejectedExecutionException {
				if (pendingCalls < maxParallelCalls) {
					pendingCalls++;
					return supplier.get().whenComplete(($, e) -> {
						pendingCalls--;
						processBuffer();
					});
				}
				if (deque.size() > maxBufferedCalls) {
					throw new RejectedExecutionException();
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
				return Promise.ofCallback(settablePromise ->
						retryImpl(supplier, retryPolicy, 0, 0, settablePromise));
			}
		};
	}

	private static <T> void retryImpl(@NotNull AsyncSupplier<? extends T> supplier, @NotNull RetryPolicy retryPolicy,
			int retryCount, long _retryTimestamp, @NotNull SettableCallback<T> cb) {
		supplier.get()
				.async()
				.whenComplete((value, e) -> {
					if (e == null) {
						cb.set(value);
					} else {
						Eventloop eventloop = Eventloop.getCurrentEventloop();
						long now = eventloop.currentTimeMillis();
						long retryTimestamp = _retryTimestamp != 0 ? _retryTimestamp : now;
						long nextRetryTimestamp = retryPolicy.nextRetryTimestamp(now, e, retryCount, retryTimestamp);
						if (nextRetryTimestamp == 0) {
							cb.setException(e);
						} else {
							eventloop.schedule(nextRetryTimestamp,
									() -> retryImpl(supplier, retryPolicy, retryCount + 1, retryTimestamp, cb));
						}
					}
				});
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
