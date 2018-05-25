package io.datakernel.async;

import io.datakernel.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public interface Callback<T> {
	void set(@Nullable T result);

	void setException(Throwable t);

	default void set(@Nullable T result, @Nullable Throwable throwable) {
		if (throwable == null) {
			set(result);
		} else {
			setException(throwable);
		}
	}

	static <T> void stageToCallback(Stage<T> stage, Callback<T> callback) {
		if (stage instanceof SettableStage) {
			SettableStage<T> settableStage = (SettableStage<T>) stage;
			if (settableStage.isSet()) {
				callback.set(settableStage.result, settableStage.exception);
				return;
			}
		}
		stage.whenComplete(callback::set);
	}

	Callback<Object> IGNORE_CALLBACK = new Callback<Object>() {
		@Override
		public void set(Object result) {
		}

		@Override
		public void setException(Throwable t) {
		}
	};

	@SuppressWarnings("unchecked")
	static <T> Callback<T> ignore() {
		return (Callback<T>) IGNORE_CALLBACK;
	}

	static <T> Callback<T> forBiConsumer(BiConsumer<T, Throwable> biConsumer) {
		return new Callback<T>() {
			@Override
			public void set(T result) {
				biConsumer.accept(result, null);
			}

			@Override
			public void setException(Throwable t) {
				biConsumer.accept(null, t);
			}
		};
	}

	static <T> Callback<T> forFuture(CompletableFuture<T> future) {
		return new Callback<T>() {
			@Override
			public void set(T result) {
				future.complete(result);
			}

			@Override
			public void setException(Throwable t) {
				future.completeExceptionally(t);
			}
		};
	}

	static <T> Callback<T> assertNoExceptions() {
		return new Callback<T>() {
			@Override
			public void set(T result) {
			}

			@Override
			public void setException(Throwable t) {
				throw new AssertionError(t);
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
			public void setException(Throwable t) {
				throw new AssertionError(t);
			}
		};
	}
}
