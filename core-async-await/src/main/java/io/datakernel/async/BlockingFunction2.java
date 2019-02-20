package io.datakernel.async;

public interface BlockingFunction2<T1, T2, R> {
	R call(T1 arg1, T2 arg2) throws Exception;
}
