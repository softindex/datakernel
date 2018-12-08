package io.datakernel.async;

import io.datakernel.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

public interface Callback<T> {
	void set(@Nullable T result);

	void setException(Throwable e);

	Callback<Object> IGNORE_CALLBACK = new Callback<Object>() {
		@Override
		public void set(Object result) {
		}

		@Override
		public void setException(Throwable e) {
		}
	};

	@SuppressWarnings("unchecked")
	static <T> Callback<T> ignore() {
		return (Callback<T>) IGNORE_CALLBACK;
	}

	static <T> Callback<T> forFuture(CompletableFuture<T> future) {
		return new Callback<T>() {
			@Override
			public void set(T result) {
				future.complete(result);
			}

			@Override
			public void setException(Throwable e) {
				future.completeExceptionally(e);
			}
		};
	}

	static <T> Callback<T> assertNoCalls() {
		return new Callback<T>() {
			@Override
			public void set(T result) {
				throw new AssertionError();
			}

			@Override
			public void setException(Throwable e) {
				throw new AssertionError(e);
			}
		};
	}
}
