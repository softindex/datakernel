package io.datakernel.service;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Utils {

	public static CompletableFuture<Void> combineAll(List<? extends CompletionStage<?>> futures) {
		if (futures.isEmpty()) return CompletableFuture.completedFuture(null);
		CompletableFuture<Void> result = new CompletableFuture<>();
		AtomicInteger count = new AtomicInteger(futures.size());
		AtomicReference<Throwable> exception = new AtomicReference<>();
		for (CompletionStage<?> future : futures) {
			future.whenComplete(($, e) -> {
				if (e != null) {
					exception.compareAndSet(null, e);
				}
				if (count.decrementAndGet() == 0) {
					if (exception.get() == null) {
						result.complete(null);
					} else {
						result.completeExceptionally(exception.get());
					}
				}
			});
		}
		return result;
	}

	@NotNull
	public static <T> CompletableFuture<T> completedExceptionallyFuture(Throwable e) {
		CompletableFuture<T> future = new CompletableFuture<>();
		future.completeExceptionally(e);
		return future;
	}

}
