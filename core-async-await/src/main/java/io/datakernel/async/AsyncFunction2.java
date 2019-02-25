package io.datakernel.async;

public interface AsyncFunction2<T1, T2, R> {
	Promise<R> call(T1 arg1, T2 arg2);
}
