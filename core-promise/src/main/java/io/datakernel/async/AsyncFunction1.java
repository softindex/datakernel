package io.datakernel.async;

public interface AsyncFunction1<T1, R> {
	Promise<R> call(T1 arg1);
}
