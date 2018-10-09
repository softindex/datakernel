package io.datakernel.async;

public final class ResultWithStage<T, V> {
	private final T result;
	private final Stage<V> stage;

	private ResultWithStage(T result, Stage<V> stage) {
		this.result = result;
		this.stage = stage;
	}

	public static <T, V> ResultWithStage<T, V> of(T result, Stage<V> stage) {
		return new ResultWithStage<T, V>(result, stage);
	}

	public T getResult() {
		return result;
	}

	public Stage<V> getStage() {
		return stage;
	}
}
