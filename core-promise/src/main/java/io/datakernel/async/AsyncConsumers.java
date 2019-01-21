package io.datakernel.async;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public final class AsyncConsumers {
	private AsyncConsumers() {}

	@Contract(pure = true)
	@NotNull
	public static <T> AsyncConsumer<T> buffered(@NotNull AsyncConsumer<T> actual) {
		return buffered(1, Integer.MAX_VALUE, actual);
	}

	@Contract(pure = true)
	@NotNull
	public static <T> AsyncConsumer<T> buffered(int maxParallelCalls, int maxBufferedCalls, @NotNull AsyncConsumer<T> asyncConsumer) {
		return asyncConsumer.withExecutor(AsyncExecutors.buffered(maxParallelCalls, maxBufferedCalls));
	}

}
