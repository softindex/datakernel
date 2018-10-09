package io.datakernel.async;

public final class CompleteResultStage<T> extends CompleteStage<T> {
	private final T result;

	public CompleteResultStage(T result) {
		this.result = result;
	}

	@Override
	public final T getResult() {
		return result;
	}

}
