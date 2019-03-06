package io.datakernel.async;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.function.Consumer;

public final class AsyncSuppliers {
	private AsyncSuppliers() {}

	@Contract(pure = true)
	@NotNull
	public static <T> AsyncSupplier<T> reuse(@NotNull AsyncSupplier<? extends T> actual) {
		return new AsyncSupplierReuse<>(actual);
	}

	@Contract(pure = true)
	@NotNull
	public static <T> AsyncSupplier<T> subscribe(@NotNull AsyncSupplier<T> actual) {
		return new AsyncSupplierSubscribe<>(actual);
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
	public static <T> AsyncSupplier<T> prefetch(int count, @NotNull AsyncSupplier<? extends T> asyncSupplier) {
		return prefetch(count, asyncSupplier, asyncSupplier);
	}

	public static <T> AsyncSupplier<T> retry(AsyncSupplier<T> asyncSupplier, RetryPolicy retryPolicy) {
		return asyncSupplier.withExecutor(AsyncExecutors.retry(retryPolicy));
	}

	public static <T> AsyncSupplier<T> retry(AsyncSupplier<T> asyncSupplier) {
		return new AsyncSupplier<T>() {
			@Override
			public @NotNull Promise<T> get() {
				while (true) {
					Promise<T> promise = asyncSupplier.get();
					if (promise.isResult()) return promise;
					if (promise.isException()) continue;
					return Promise.ofCallback(cb -> getImpl(promise, cb));
				}
			}

			private void getImpl(Promise<T> promise, SettablePromise<T> cb) {
				promise.whenComplete((v, e) -> {
					if (e == null) {
						cb.set(v);
					} else {
						getImpl(asyncSupplier.get(), cb);
					}
				});
			}
		};
	}

	@Contract(pure = true)
	@SuppressWarnings("unchecked")
	public static <T> AsyncSupplier<T> prefetch(int count, @NotNull AsyncSupplier<? extends T> actualSupplier,
			@NotNull AsyncSupplier<? extends T> prefetchSupplier) {
		return count == 0 ?
				(AsyncSupplier<T>) actualSupplier :
				new AsyncSupplierPrefetch<>(actualSupplier, prefetchSupplier, count);
	}

	public static final class AsyncSupplierPrefetch<T> implements AsyncSupplier<T> {
		private final int targetCount;
		@NotNull
		private final AsyncSupplier<? extends T> actualSupplier;
		@NotNull
		private final AsyncSupplier<? extends T> prefetchCallable;

		private final ArrayDeque<T> prefetched = new ArrayDeque<>();
		private int prefetchCalls;

		public AsyncSupplierPrefetch(@NotNull AsyncSupplier<? extends T> actualSupplier, int count) {
			this(actualSupplier, actualSupplier, count);
		}

		public AsyncSupplierPrefetch(@NotNull AsyncSupplier<? extends T> actualSupplier, @NotNull AsyncSupplier<? extends T> prefetchSupplier, int count) {
			this.actualSupplier = actualSupplier;
			this.prefetchCallable = prefetchSupplier;
			this.targetCount = count;
		}

		@NotNull
		@SuppressWarnings("unchecked")
		@Override
		public Promise<T> get() {
			Promise<? extends T> result = prefetched.isEmpty() ? actualSupplier.get() : Promise.of(prefetched.pollFirst());
			prefetch();
			return (Promise<T>) result;
		}

		public void prefetch() {
			for (int i = 0; i < targetCount - (prefetched.size() + prefetchCalls); i++) {
				prefetchCalls++;
				prefetchCallable.get()
						.async()
						.whenComplete((value, e) -> {
							prefetchCalls--;
							if (e == null) {
								prefetched.addLast(value);
							}
						});
			}
		}

		public int getTargetCount() {
			return targetCount;
		}

		public int getPrefetchCount() {
			return prefetched.size();
		}

		public int getPrefetchCalls() {
			return prefetchCalls;
		}

		public void drainTo(Consumer<T> consumer) {
			prefetched.forEach(consumer);
			prefetched.clear();
		}
	}

	public static class AsyncSupplierSubscribe<T> implements AsyncSupplier<T> {
		boolean isRunning;

		@Nullable
		SettablePromise<T> subscribedPromise;

		@NotNull
		private final AsyncSupplier<T> actualSupplier;

		public AsyncSupplierSubscribe(@NotNull AsyncSupplier<T> actualSupplier) {this.actualSupplier = actualSupplier;}

		@NotNull
		@Override
		public Promise<T> get() {
			if (!isRunning) {
				assert subscribedPromise == null;
				SettablePromise<T> result = new SettablePromise<>();
				isRunning = true;
				Promise<? extends T> promise = actualSupplier.get();
				promise.whenComplete((v, e) -> {
					result.set(v, e);
					isRunning = false;
					processSubscribed();
				});
				return result;
			}
			if (subscribedPromise == null) {
				subscribedPromise = new SettablePromise<>();
			}
			return subscribedPromise;
		}

		private void processSubscribed() {
			while (this.subscribedPromise != null) {
				SettablePromise<T> subscribedPromise = this.subscribedPromise;
				this.subscribedPromise = null;
				isRunning = true;
				Promise<? extends T> promise = actualSupplier.get();
				if (promise.isComplete()) {
					promise.whenComplete(subscribedPromise::set);
					isRunning = false;
					continue;
				}
				promise.whenComplete((result, e) -> {
					subscribedPromise.set(result, e);
					isRunning = false;
					processSubscribed();
				});
				break;
			}
		}

		public boolean isRunning() {
			return isRunning;
		}

		public boolean isSubscribed() {
			return subscribedPromise != null;
		}
	}

	public static final class AsyncSupplierReuse<T> implements AsyncSupplier<T> {
		@Nullable
		Promise<T> runningPromise;

		@NotNull
		private final AsyncSupplier<? extends T> actualSupplier;

		public AsyncSupplierReuse(@NotNull AsyncSupplier<? extends T> actualSupplier) {this.actualSupplier = actualSupplier;}

		@NotNull
		@SuppressWarnings("unchecked")
		@Override
		public Promise<T> get() {
			if (runningPromise != null) return runningPromise;
			runningPromise = (Promise<T>) actualSupplier.get();
			Promise<T> runningPromise = this.runningPromise;
			runningPromise.whenComplete((result, e) -> this.runningPromise = null);
			return runningPromise;
		}

		public boolean isRunning() {
			return runningPromise != null;
		}
	}

	public static final class AsyncSupplierWithStatus<T> implements AsyncSupplier<T> {
		@NotNull
		private final AsyncSupplier<T> asyncSupplier;

		private int running;

		public AsyncSupplierWithStatus(@NotNull AsyncSupplier<T> asyncSupplier) {this.asyncSupplier = asyncSupplier;}

		@Override
		public @NotNull Promise<T> get() {
			running++;
			return asyncSupplier.get()
					.whenComplete((v, e) -> running--);
		}

		public int getRunning() {
			return running;
		}

		public boolean isRunning() {
			return running != 0;
		}
	}

}
