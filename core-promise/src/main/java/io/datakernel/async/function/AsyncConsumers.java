package io.datakernel.async.function;

import io.datakernel.async.process.AsyncExecutors;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public final class AsyncConsumers {
	private AsyncConsumers() {}

	@Contract(pure = true)
	@NotNull
	public static <T> AsyncConsumer<T> buffer(@NotNull AsyncConsumer<T> actual) {
		return buffer(1, Integer.MAX_VALUE, actual);
	}

	@Contract(pure = true)
	@NotNull
	public static <T> AsyncConsumer<T> buffer(int maxParallelCalls, int maxBufferedCalls, @NotNull AsyncConsumer<T> asyncConsumer) {
		return asyncConsumer.withExecutor(AsyncExecutors.buffered(maxParallelCalls, maxBufferedCalls));
	}

}
