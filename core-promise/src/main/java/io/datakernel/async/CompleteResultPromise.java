package io.datakernel.async;

/**
 * Represents a {@link CompletePromise} with a result of unspecified type.
 * @param <T> type of the result
 */
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
