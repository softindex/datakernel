package io.datakernel.async;

import java.util.ArrayDeque;
import java.util.concurrent.RejectedExecutionException;

public final class AsyncSuppliers {
	private AsyncSuppliers() {
	}

	public static <T> AsyncSupplier<T> reuse(AsyncSupplier<? extends T> actual) {
		return new AsyncSupplier<T>() {
			Stage<T> runningStage;

			@SuppressWarnings("unchecked")
			@Override
			public Stage<T> get() {
				if (runningStage != null) return runningStage;
				runningStage = (Stage<T>) actual.get();
				runningStage.whenComplete((result, throwable) -> runningStage = null);
				return runningStage;
			}
		};
	}

	public static <T> AsyncSupplier<T> resubscribe(AsyncSupplier<? extends T> actual) {
		return new AsyncSupplier<T>() {
			SettableStage<T> runningStage;
			SettableStage<T> subscribeStage;

			@Override
			public Stage<T> get() {
				if (runningStage == null) {
					assert subscribeStage == null;
					runningStage = new SettableStage<>();
					runningStage.whenComplete((result, throwable) -> {
						runningStage = subscribeStage;
						subscribeStage = null;
						actual.get().whenComplete(runningStage::set);
					});
					actual.get().whenComplete(runningStage::set);
					return runningStage;
				}
				if (subscribeStage == null) {
					subscribeStage = new SettableStage<>();
				}
				return subscribeStage;
			}
		};
	}

	public static <T> AsyncSupplier<T> buffered(AsyncSupplier<? extends T> actual) {
		return buffered(1, Integer.MAX_VALUE, actual);
	}

	public static <T> AsyncSupplier<T> buffered(int maxParallelCalls, int maxBufferedCalls, AsyncSupplier<? extends T> actual) {
		return new AsyncSupplier<T>() {
			private int pendingCalls;
			private final ArrayDeque<SettableStage<T>> deque = new ArrayDeque<>();

			@SuppressWarnings("ConstantConditions")
			private void processQueue() {
				while (pendingCalls < maxParallelCalls && !deque.isEmpty()) {
					SettableStage<T> resultStage = deque.pollFirst();
					pendingCalls++;
					actual.get().whenComplete((value, throwable) -> {
						pendingCalls--;
						processQueue();
						resultStage.set(value, throwable);
					});
				}
			}

			@SuppressWarnings("unchecked")
			@Override
			public Stage<T> get() {
				if (pendingCalls <= maxParallelCalls) {
					pendingCalls++;
					return (Stage<T>) actual.get().whenComplete((value, throwable) -> {
						pendingCalls--;
						processQueue();
					});
				}
				if (deque.size() > maxBufferedCalls) {
					return Stage.ofException(new RejectedExecutionException());
				}
				SettableStage<T> result = new SettableStage<>();
				deque.addLast(result);
				return result;
			}
		};
	}

	@SuppressWarnings("unchecked")
	public static <T> AsyncSupplier<T> prefetch(int count, AsyncSupplier<? extends T> actual) {
		return prefetch(count, actual, actual);
	}

	@SuppressWarnings("unchecked")
	public static <T> AsyncSupplier<T> prefetch(int count, AsyncSupplier<? extends T> actual, AsyncSupplier<? extends T> prefetchCallable) {
		return count == 0 ? (AsyncSupplier<T>) actual : new AsyncSupplier<T>() {
			private int pendingCalls;
			private final ArrayDeque<T> deque = new ArrayDeque<>();

			private void tryPrefetch() {
				for (int i = 0; i < count - (deque.size() + pendingCalls); i++) {
					pendingCalls++;
					prefetchCallable.get().whenComplete((value, throwable) -> {
						pendingCalls--;
						if (throwable == null) {
							deque.addLast(value);
						}
					});
				}
			}

			@SuppressWarnings("unchecked")
			@Override
			public Stage<T> get() {
				Stage<? extends T> result = deque.isEmpty() ? actual.get() : Stage.of(deque.pollFirst());
				tryPrefetch();
				return (Stage<T>) result;
			}
		};
	}
}
