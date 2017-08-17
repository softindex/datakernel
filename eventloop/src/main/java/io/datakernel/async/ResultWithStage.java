package io.datakernel.async;

import java.util.concurrent.CompletionStage;

public final class ResultWithStage<T, V> {
	private final T result;
	private final CompletionStage<V> stage;

	private ResultWithStage(T result, CompletionStage<V> stage) {
		this.result = result;
		this.stage = stage;
	}

	public static <T, V> ResultWithStage<T, V> of(T result, CompletionStage<V> stage) {
		return new ResultWithStage<T, V>(result, stage);
	}

	public T getResult() {
		return result;
	}

	public CompletionStage<V> getStage() {
		return stage;
	}
}
