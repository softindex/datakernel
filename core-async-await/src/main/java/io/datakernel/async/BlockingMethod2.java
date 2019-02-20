package io.datakernel.async;

public interface BlockingMethod2<T1, T2> {
	void call(T1 arg1, T2 arg2) throws Exception;
}
