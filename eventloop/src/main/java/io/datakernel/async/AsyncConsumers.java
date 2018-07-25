package io.datakernel.async;

public final class AsyncConsumers {
	private AsyncConsumers() {
	}

	public static <T> AsyncConsumer<T> buffered(AsyncConsumer<T> actual) {
		return buffered(1, Integer.MAX_VALUE, actual);
	}

	public static <T> AsyncConsumer<T> buffered(int maxParallelCalls, int maxBufferedCalls, AsyncConsumer<T> asyncConsumer) {
		return asyncConsumer.withExecutor(AsyncExecutors.buffered(maxParallelCalls, maxBufferedCalls));
	}

}
