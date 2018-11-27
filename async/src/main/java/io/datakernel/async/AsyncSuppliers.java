package io.datakernel.async;

import io.datakernel.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.concurrent.RejectedExecutionException;

public final class AsyncSuppliers {
	private AsyncSuppliers() {
	}

	public static <T> AsyncSupplier<T> reuse(AsyncSupplier<? extends T> actual) {
		return new AsyncSupplier<T>() {
			@Nullable
			Promise<T> runningPromise;

			@SuppressWarnings("unchecked")
			@Override
			public Promise<T> get() {
				if (runningPromise != null) return runningPromise;
				runningPromise = (Promise<T>) actual.get();
				Promise<T> runningPromise = this.runningPromise;
				runningPromise.whenComplete((result, e) -> this.runningPromise = null);
				return runningPromise;
			}
		};
	}

	public static <T> AsyncSupplier<T> resubscribe(AsyncSupplier<? extends T> actual) {
		return new AsyncSupplier<T>() {
			SettablePromise<T> runningPromise;
			@Nullable
			SettablePromise<T> subscribePromise;

			@Override
			public Promise<T> get() {
				if (runningPromise == null) {
					assert subscribePromise == null;
					runningPromise = new SettablePromise<>();
					runningPromise.whenComplete((result, e) -> {
						runningPromise = subscribePromise;
						if (runningPromise == null) return;
						subscribePromise = null;
						actual.get().async().whenComplete(runningPromise::set);
					});
					actual.get().async().whenComplete(runningPromise::set);
					return runningPromise;
				}
				if (subscribePromise == null) {
					subscribePromise = new SettablePromise<>();
				}
				return subscribePromise;
			}
		};
	}

	public static <T> AsyncSupplier<T> buffered(AsyncSupplier<? extends T> actual) {
		return buffered(1, Integer.MAX_VALUE, actual);
	}

	public static <T> AsyncSupplier<T> buffered(int maxParallelCalls, int maxBufferedCalls, AsyncSupplier<? extends T> actual) {
		return new AsyncSupplier<T>() {
			private int pendingCalls;
			private final ArrayDeque<SettablePromise<T>> deque = new ArrayDeque<>();

			@SuppressWarnings("ConstantConditions")
			private void processQueue() {
				while (pendingCalls < maxParallelCalls && !deque.isEmpty()) {
					SettablePromise<T> resultPromise = deque.pollFirst();
					pendingCalls++;
					actual.get().async().whenComplete((value, e) -> {
						pendingCalls--;
						processQueue();
						resultPromise.set(value, e);
					});
				}
			}

			@SuppressWarnings("unchecked")
			@Override
			public Promise<T> get() {
				if (pendingCalls <= maxParallelCalls) {
					pendingCalls++;
					return (Promise<T>) actual.get().async().whenComplete((value, e) -> {
						pendingCalls--;
						processQueue();
					});
				}
				if (deque.size() > maxBufferedCalls) {
					return Promise.ofException(new RejectedExecutionException());
				}
				SettablePromise<T> result = new SettablePromise<>();
				deque.addLast(result);
				return result;
			}
		};
	}

	public static <T> AsyncSupplier<T> prefetch(int count, AsyncSupplier<? extends T> actual) {
		return prefetch(count, actual, actual);
	}

	@SuppressWarnings("unchecked")
	public static <T> AsyncSupplier<T> prefetch(int count, AsyncSupplier<? extends T> actual, AsyncSupplier<? extends T> prefetchCallable) {
		return count == 0 ?
				(AsyncSupplier<T>) actual :
				new AsyncSupplier<T>() {
					private int pendingCalls;
					private final ArrayDeque<T> deque = new ArrayDeque<>();

					private void tryPrefetch() {
						for (int i = 0; i < count - (deque.size() + pendingCalls); i++) {
							pendingCalls++;
							prefetchCallable.get().async().whenComplete((value, e) -> {
								pendingCalls--;
								if (e == null) {
									deque.addLast(value);
								}
							});
						}
					}

					@SuppressWarnings("unchecked")
					@Override
					public Promise<T> get() {
						Promise<? extends T> result = deque.isEmpty() ? actual.get() : Promise.of(deque.pollFirst());
						tryPrefetch();
						return (Promise<T>) result;
					}
				};
	}

}
