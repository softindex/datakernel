package io.datakernel.codec;

public interface StructuredEncoder<T> {
	void encode(StructuredOutput out, T item);

	static <T> StructuredEncoder<T> ofObject(StructuredEncoder<T> encoder) {
		return (out, item) -> out.writeObject(encoder, item);
	}

	static <T> StructuredEncoder<T> ofObject() {
		return ofObject((out, item) -> {});
	}

	static <T> StructuredEncoder<T> ofTuple(StructuredEncoder<T> encoder) {
		return (out, item) -> out.writeTuple(encoder, item);
	}

}
