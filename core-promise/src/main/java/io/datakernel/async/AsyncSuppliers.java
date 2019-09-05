package io.datakernel.async;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.function.Function;

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
	public static <T> AsyncSupplier<T> coalesce(@NotNull AsyncSupplier<T> actual) {
		Function<Void, Promise<T>> fn = Promises.coalesce(v -> actual.get(), a -> actual.get(), () -> null, (a, v) -> {});
		return () -> fn.apply(null);
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

			void getImpl(Promise<T> promise, SettablePromise<T> cb) {
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
	public static <T> AsyncSupplier<T> prefetch(int count, @NotNull AsyncSupplier<? extends T> asyncSupplier) {
		return prefetch(count, asyncSupplier, asyncSupplier);
	}

	@Contract(pure = true)
	@SuppressWarnings("unchecked")
	public static <T> AsyncSupplier<T> prefetch(int count,
			@NotNull AsyncSupplier<? extends T> actualSupplier,
			@NotNull AsyncSupplier<? extends T> prefetchSupplier) {
		if (count == 0) return (AsyncSupplier<T>) actualSupplier;
		return new AsyncSupplier<T>() {
			final ArrayDeque<T> prefetched = new ArrayDeque<>();
			int prefetchCalls;

			@NotNull
			@SuppressWarnings("unchecked")
			@Override
			public Promise<T> get() {
				Promise<? extends T> result = prefetched.isEmpty() ? actualSupplier.get() : Promise.of(prefetched.pollFirst());
				prefetch();
				return (Promise<T>) result;
			}

			void prefetch() {
				for (int i = 0; i < count - (prefetched.size() + prefetchCalls); i++) {
					prefetchCalls++;
					prefetchSupplier.get()
							.async()
							.whenComplete((value, e) -> {
								prefetchCalls--;
								if (e == null) {
									prefetched.addLast(value);
								}
							});
				}
			}
		};
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
