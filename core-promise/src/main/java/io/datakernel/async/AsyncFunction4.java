package io.datakernel.async;

public interface AsyncFunction4<T1, T2, T3, T4, R> {
	Promise<R> call(T1 arg1, T2 arg2, T3 arg3, T4 arg4);
}
