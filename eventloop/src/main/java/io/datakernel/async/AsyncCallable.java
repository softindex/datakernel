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

package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;

import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public interface AsyncCallable<T> {
	Stage<T> call();

	default AsyncCallable<T> with(UnaryOperator<AsyncCallable<T>> modifier) {
		return modifier.apply(this);
	}

	static <T> AsyncCallable<T> of(Supplier<Stage<T>> supplier) {
		return supplier::get;
	}

	static <A, T> AsyncCallable<T> of(AsyncFunction<? super A, T> function, A a) {
		return () -> function.apply(a);
	}

	static <A, B, T> AsyncCallable<T> of(BiFunction<? super A, ? super B, Stage<T>> biFunction, A a, B b) {
		return () -> biFunction.apply(a, b);
	}

	default AsyncCallable<T> sharedCall() {
		return sharedCall(this);
	}

	static <T> AsyncCallable<T> sharedCall(AsyncCallable<T> actualCallable) {
		return new AsyncCallable<T>() {
			SettableStage<T> runningStage;

			@Override
			public Stage<T> call() {
				if (runningStage != null)
					return runningStage;
				runningStage = SettableStage.create();
				runningStage.whenComplete((result, throwable) -> runningStage = null);
				actualCallable.call().whenComplete(runningStage::set);
				return runningStage;
			}
		};
	}

	default AsyncCallable<T> singleCall() {
		return maxCalls(1);
	}

	default AsyncCallable<T> singleCall(int maxQueueSize) {
		return maxCalls(1, maxQueueSize);
	}

	default AsyncCallable<T> maxCalls(int maxParallelCalls) {
		return maxCalls(maxParallelCalls, Integer.MAX_VALUE);
	}

	default AsyncCallable<T> maxCalls(int maxParallelCalls, int maxQueueSize) {
		return maxCalls(this, maxParallelCalls, maxQueueSize);
	}

	static <T> AsyncCallable<T> maxCalls(AsyncCallable<T> actualCallable, int maxParallelCalls, int maxQueueSize) {
		return new AsyncCallable<T>() {
			private int pendingCalls;
			private final ArrayDeque<SettableStage<T>> deque = new ArrayDeque<>();

			void processQueue() {
				while (pendingCalls < maxParallelCalls && !deque.isEmpty()) {
					SettableStage<T> settableStage = deque.pollFirst();
					pendingCalls++;
					actualCallable.call().whenComplete((value, throwable) -> {
						pendingCalls--;
						processQueue();
						settableStage.set(value, throwable);
					});
				}
			}

			@Override
			public Stage<T> call() {
				if (pendingCalls <= maxParallelCalls) {
					pendingCalls++;
					return actualCallable.call().whenComplete((value, throwable) -> {
						pendingCalls--;
						processQueue();
					});
				}
				if (deque.size() > maxQueueSize) {
					return Stage.ofException(new IllegalStateException());
				}
				SettableStage<T> result = SettableStage.create();
				deque.addLast(result);
				return result;
			}
		};
	}

	default AsyncCallable<T> retry(RetryPolicy retryPolicy) {
		return retry(this, retryPolicy);
	}

	static <T> AsyncCallable<T> retry(AsyncCallable<T> actualCallable, RetryPolicy retryPolicy) {
		return new AsyncCallable<T>() {
			void doCall(int retryCount, long _retryTimestamp, SettableStage<T> cb) {
				actualCallable.call().whenComplete((value, throwable) -> {
					if (throwable == null) {
						cb.set(value);
					} else {
						Eventloop eventloop = Eventloop.getCurrentEventloop();
						long now = eventloop.currentTimeMillis();
						long retryTimestamp = _retryTimestamp != 0 ? _retryTimestamp : now;
						long nextRetryTimestamp = retryPolicy.nextRetryTimestamp(now, throwable, retryCount, retryTimestamp);
						if (nextRetryTimestamp == 0) {
							cb.setException(throwable);
						} else {
							eventloop.schedule(nextRetryTimestamp,
									() -> doCall(retryCount + 1, retryTimestamp, cb));
						}
					}
				});
			}

			@Override
			public Stage<T> call() {
				SettableStage<T> result = SettableStage.create();
				doCall(0, 0, result);
				return result;
			}
		};
	}

	default AsyncCallable<T> prefetch(int maxSize) {
		return prefetch(this, this, maxSize);
	}

	static <T> AsyncCallable<T> prefetch(AsyncCallable<T> actualCallable, AsyncCallable<T> prefetchCallable, int maxSize) {
		return new AsyncCallable<T>() {
			private int pendingCalls;
			private final ArrayDeque<T> deque = new ArrayDeque<>();

			private void tryPrefetch() {
				for (int i = 0; i < maxSize - (deque.size() + pendingCalls); i++) {
					pendingCalls++;
					prefetchCallable.call().whenComplete((value, throwable) -> {
						pendingCalls--;
						if (throwable == null) {
							deque.addLast(value);
						}
					});
				}
			}

			@Override
			public Stage<T> call() {
				Stage<T> result = deque.isEmpty() ? actualCallable.call() : Stage.of(deque.pollFirst());
				tryPrefetch();
				return result;
			}
		};
	}

	default <U> AsyncCallable<U> handle(Stage.Handler<? super T, U> fn) {
		return () -> call().handle(fn);
	}

	default <U> AsyncCallable<U> handleAsync(Stage.Handler<? super T, U> fn) {
		return () -> call().handleAsync(fn);
	}

	default <U> AsyncCallable<U> handleAsync(Stage.Handler<? super T, U> fn, Executor executor) {
		return () -> call().handleAsync(fn, executor);
	}

	default <V> AsyncCallable<V> thenApply(Function<? super T, ? extends V> function) {
		return () -> call().thenApply(function);
	}

	default AsyncCallable<T> whenComplete(StageConsumer<? super T> action) {
		return () -> call().whenComplete(action);
	}
}