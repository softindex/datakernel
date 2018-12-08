package io.datakernel.async;

public final class CompleteResultPromise<T> extends CompletePromise<T> {
	private final T result;

	public CompleteResultPromise(T result) {
		this.result = result;
	}

	@Override
	public final T getResult() {
		return result;
	}

}
