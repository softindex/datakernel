package io.datakernel.async;

import java.util.ArrayDeque;
import java.util.concurrent.RejectedExecutionException;

import static io.datakernel.async.Utils.retryImpl;

public final class AsyncConsumers {
	private AsyncConsumers() {
	}

	public static <T> AsyncConsumer<T> buffer(AsyncConsumer<? super T> actual) {
		return buffer(1, Integer.MAX_VALUE, actual);
	}

	public static <T> AsyncConsumer<T> buffer(int maxParallelCalls, int maxBufferSize, AsyncConsumer<? super T> actual) {
		return new AsyncConsumer<T>() {
			private int pendingCalls;
			private final ArrayDeque<Object> deque = new ArrayDeque<>();

			@SuppressWarnings({"unchecked", "ConstantConditions"})
			private void processBuffer() {
				while (pendingCalls < maxParallelCalls && !deque.isEmpty()) {
					T value = (T) deque.pollFirst();
					SettableStage<Void> settableStage = (SettableStage<Void>) deque.pollFirst();
					pendingCalls++;
					actual.accept(value).whenComplete(($, throwable) -> {
						pendingCalls--;
						processBuffer();
						settableStage.set(null, throwable);
					});
				}
			}

			@Override
			public Stage<Void> accept(T value) throws RejectedExecutionException {
				if (pendingCalls <= maxParallelCalls) {
					pendingCalls++;
					return actual.accept(value).whenComplete(($, throwable) -> {
						pendingCalls--;
						processBuffer();
					});
				}
				if (deque.size() > maxBufferSize) {
					throw new RejectedExecutionException();
				}
				SettableStage<Void> result = new SettableStage<>();
				deque.addLast(value);
				deque.addLast(result);
				return result;
			}
		};

	}

	public static <T> AsyncConsumer<T> retry(RetryPolicy retryPolicy, AsyncConsumer<? super T> actual) {
		return value -> {
			SettableStage<Void> result = new SettableStage<>();
			retryImpl(() -> actual.accept(value), retryPolicy, 0, 0, result);
			return result;
		};
	}
}
