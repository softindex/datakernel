package io.datakernel.codec;

public interface StructuredEncoder<T> {
	void encode(StructuredOutput out, T item);
}
