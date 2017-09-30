package io.datakernel.async;

public interface IndexedReducer<A, T> {
	void accumulate(A accumulator, T item, int index);
}
