package io.datakernel.service.util;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Utils {

	public static CompletableFuture<Void> combineAll(List<? extends CompletionStage<?>> futures) {
		if (futures.isEmpty()) return CompletableFuture.completedFuture(null);
		CompletableFuture<Void> result = new CompletableFuture<>();
		AtomicInteger count = new AtomicInteger(futures.size());
		List<Throwable> exceptions = new ArrayList<>();
		for (CompletionStage<?> future : futures) {
			future.whenComplete(($, e) -> {
				if (e != null) {
					synchronized (exceptions) {
						exceptions.add(Stream.iterate(e, Throwable::getCause).filter(_e -> _e.getCause() != null).findFirst().get());
					}
				}
				if (count.decrementAndGet() == 0) {
					if (exceptions.isEmpty()) {
						result.complete(null);
					} else {
						result.completeExceptionally(exceptions.get(0));
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
