package io.global.ot.server;

import io.datakernel.async.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Predicate;

public final class LongPolling<T> {
	@NotNull
	private final AsyncSupplier<T> fetch;

	@NotNull
	private final AsyncSupplier<T> fetchNext;

	@Nullable
	private SettablePromise<T> promise;

	public LongPolling(@NotNull AsyncSupplier<T> fetch) {
		this.fetch = AsyncSuppliers.coalesce(fetch);
		this.fetchNext = () -> {
			if (promise != null) return promise;
			promise = new SettablePromise<>();
			return promise
					.whenComplete((v, e) -> promise = null);
		};
	}

	public Promise<T> poll(@NotNull Predicate<T> predicate) {
		return fetch.get()
				.thenCompose(initialValue -> Promises.until(initialValue,
						$ -> fetchNext.get(),
						AsyncPredicate.of(predicate)));
	}

	public void wakeup(T value) {
		if (promise != null) {
			promise.set(value);
		}
	}

	public void wakeup() {
		if (promise != null) {
			fetch.get().whenResult(this::wakeup);
		}
	}
}
