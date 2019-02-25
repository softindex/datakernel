package io.datakernel.async;

public interface BlockingFunction4<T1, T2, T3, T4, R> {
	R call(T1 arg1, T2 arg2, T3 arg3, T4 arg4) throws Exception;
}
