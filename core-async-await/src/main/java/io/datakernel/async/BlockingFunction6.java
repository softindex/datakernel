package io.datakernel.async;

public interface BlockingFunction6<T1, T2, T3, T4, T5, T6, R> {
	R call(T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6) throws Exception;
}
