package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

public class AsyncExecutors {
	private AsyncExecutors() {}

	public static AsyncExecutor direct() {
		return new AsyncExecutor() {
			@Override
			public <T> Stage<T> execute(AsyncSupplier<T> supplier) {
				return supplier.get();
			}
		};
	}

	public static AsyncExecutor ofEventloop(Eventloop eventloop) {
		return new AsyncExecutor() {
			@Override
			public <T> Stage<T> execute(AsyncSupplier<T> supplier) {
				Eventloop currentEventloop = Eventloop.getCurrentEventloop();
				if (eventloop == currentEventloop) {
					return supplier.get();
				}
				return Stage.ofCallback(cb -> {
					currentEventloop.startExternalTask();
					eventloop.execute(() -> supplier.get()
							.whenComplete((result, throwable) -> {
								currentEventloop.execute(() ->
										cb.set(result, throwable));
								currentEventloop.completeExternalTask();
							}));
				});
			}
		};
	}

	public static AsyncExecutor roundRobin(List<AsyncExecutor> executors) {
		return new AsyncExecutor() {
			int index;

			@Override
			public <T> Stage<T> execute(AsyncSupplier<T> supplier) {
				AsyncExecutor executor = executors.get(index);
				index = (index + 1) % executors.size();
				return executor.execute(supplier);
			}
		};
	}

	public static AsyncExecutor buffered() {
		return buffered(1, Integer.MAX_VALUE);
	}

	public static AsyncExecutor buffered(int maxParallelCalls, int maxBufferedCalls) {
		return new AsyncExecutor() {
			private int pendingCalls;
			private final ArrayDeque<Object> deque = new ArrayDeque<>();

			@SuppressWarnings({"unchecked", "ConstantConditions"})
			private void processBuffer() {
				while (pendingCalls < maxParallelCalls && !deque.isEmpty()) {
					AsyncSupplier<Object> supplier = (AsyncSupplier<Object>) deque.pollFirst();
					SettableStage<Object> settableStage = (SettableStage<Object>) deque.pollFirst();
					pendingCalls++;
					supplier.get().whenComplete((result, throwable) -> {
						pendingCalls--;
						processBuffer();
						settableStage.set(result, throwable);
					});
				}
			}

			@Override
			public <T> Stage<T> execute(AsyncSupplier<T> supplier) throws RejectedExecutionException {
				if (pendingCalls <= maxParallelCalls) {
					pendingCalls++;
					return supplier.get().async().whenComplete(($, throwable) -> {
						pendingCalls--;
						processBuffer();
					});
				}
				if (deque.size() > maxBufferedCalls) {
					throw new RejectedExecutionException();
				}
				SettableStage<T> result = new SettableStage<>();
				deque.addLast(supplier);
				deque.addLast(result);
				return result;
			}
		};
	}

	public static AsyncExecutor retry(RetryPolicy retryPolicy) {
		return new AsyncExecutor() {
			@Override
			public <T> Stage<T> execute(AsyncSupplier<T> supplier) {
				return Stage.ofCallback(settableStage ->
						retryImpl(supplier, retryPolicy, 0, 0, settableStage));
			}
		};
	}

	private static <T> void retryImpl(AsyncSupplier<? extends T> supplier, RetryPolicy retryPolicy,
			int retryCount, long _retryTimestamp, SettableStage<T> cb) {
		supplier.get().async().whenComplete((value, throwable) -> {
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
							() -> retryImpl(supplier, retryPolicy, retryCount + 1, retryTimestamp, cb));
				}
			}
		});
	}
}
