package io.datakernel.async;

public interface Reducer<A, T> {
	void accumulate(A accumulator, T item);
}
