package io.datakernel.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

public interface ResultCallback<T> {
	void set(T result);

	void setException(Throwable t);

	static <T> void stageToCallback(CompletionStage<T> stage, ResultCallback<T> callback) {
		if (stage instanceof SettableStage) {
			SettableStage<T> settableStage = (SettableStage<T>) stage;
			if (settableStage.isSet()) {
				if (settableStage.exception == null) {
					callback.set(settableStage.result);
				} else {
					callback.setException(settableStage.exception);
				}
				return;
			}
		}
		stage.whenComplete((value, throwable) -> {
			if (throwable == null) {
				callback.set(value);
			} else {
				callback.setException(throwable);
			}
		});
	}

	static <T> ResultCallback<T> ignore() {
		return new ResultCallback<T>() {
			@Override
			public void set(T result) {
			}

			@Override
			public void setException(Throwable t) {
			}
		};
	}

	static <T> ResultCallback<T> forBiConsumer(BiConsumer<T, Throwable> biConsumer) {
		return new ResultCallback<T>() {
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

	static <T> ResultCallback<T> forFuture(CompletableFuture<T> future) {
		return new ResultCallback<T>() {
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

	static <T> ResultCallback<T> assertNoExceptions() {
		return new ResultCallback<T>() {
			@Override
			public void set(T result) {
			}

			@Override
			public void setException(Throwable t) {
				throw new AssertionError(t);
			}
		};
	}

	static <T> ResultCallback<T> assertNoCalls() {
		return new ResultCallback<T>() {
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
