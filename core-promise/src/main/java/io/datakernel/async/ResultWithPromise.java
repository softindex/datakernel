package io.datakernel.async;

public final class ResultWithPromise<T, V> {
	private final T result;
	private final Promise<V> promise;

	private ResultWithPromise(T result, Promise<V> promise) {
		this.result = result;
		this.promise = promise;
	}

	public static <T, V> ResultWithPromise<T, V> of(T result, Promise<V> promise) {
		return new ResultWithPromise<>(result, promise);
	}

	public T getResult() {
		return result;
	}

	public Promise<V> getPromise() {
		return promise;
	}
}
