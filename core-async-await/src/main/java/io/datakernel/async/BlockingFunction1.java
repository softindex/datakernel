package io.datakernel.async;

public interface BlockingFunction1<T1, R> {
	R call(T1 arg1) throws Exception;
}
