package io.datakernel.async;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.concurrent.RejectedExecutionException;

public final class AsyncSuppliers {
	private AsyncSuppliers() {}

	@Contract(pure = true)
	@NotNull
	public static <T> AsyncSupplier<T> reuse(@NotNull AsyncSupplier<? extends T> actual) {
		return new AsyncSupplier<T>() {
			@Nullable
			Promise<T> runningPromise;

			@NotNull
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

	@Contract(pure = true)
	@NotNull
	public static <T> AsyncSupplier<T> subscribe(@NotNull AsyncSupplier<T> actual) {
		return new AsyncSupplier<T>() {
			boolean isRunning;
			@Nullable
			SettablePromise<T> subscribedPromise;

			@NotNull
			@Override
			public Promise<T> get() {
				if (!isRunning) {
					assert subscribedPromise == null;
					SettablePromise<T> result = subscribedPromise = new SettablePromise<>();
					processSubscribedPromise();
					return result;
				}
				if (subscribedPromise == null) {
					subscribedPromise = new SettablePromise<>();
				}
				return subscribedPromise;
			}

			private void processSubscribedPromise() {
				while (this.subscribedPromise != null) {
					SettablePromise<T> subscribedPromise = this.subscribedPromise;
					this.subscribedPromise = null;
					isRunning = true;
					Promise<? extends T> promise = actual.get();
					if (promise.isComplete()) {
						promise.whenComplete(subscribedPromise::set);
						isRunning = false;
						continue;
					}
					promise.whenComplete((result, e) -> {
						subscribedPromise.set(result, e);
						isRunning = false;
						processSubscribedPromise();
					});
					break;
				}
			}
		};
	}

	@Contract(pure = true)
	@NotNull
	public static <T> AsyncSupplier<T> buffer(@NotNull AsyncSupplier<T> actual) {
		return buffer(1, Integer.MAX_VALUE, actual);
	}

	@Contract(pure = true)
	@NotNull
	public static <T> AsyncSupplier<T> buffer(int maxParallelCalls, int maxBufferedCalls, @NotNull AsyncSupplier<T> actual) {
		return actual.withExecutor(AsyncExecutors.buffered(maxParallelCalls, maxBufferedCalls));
	}

	@Contract(pure = true)
	@NotNull
	public static <T> AsyncSupplier<T> prefetch(int count, @NotNull AsyncSupplier<? extends T> actual) {
		return prefetch(count, actual, actual);
	}

	@Contract(pure = true)
	@NotNull
	@SuppressWarnings("unchecked")
	public static <T> AsyncSupplier<T> prefetch(int count, @NotNull AsyncSupplier<? extends T> actual, @NotNull AsyncSupplier<? extends T> prefetchCallable) {
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

					@NotNull
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
