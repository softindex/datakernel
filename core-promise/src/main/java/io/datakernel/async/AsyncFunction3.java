package io.datakernel.async;

public interface AsyncFunction3<T1, T2, T3, R> {
	Promise<R> call(T1 arg1, T2 arg2, T3 arg3);
}
