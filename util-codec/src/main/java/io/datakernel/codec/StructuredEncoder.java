package io.datakernel.codec;

import java.util.List;

/**
 * Encorer can write an object of type T into a {@link StructuredOutput}.
 */
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

	static <T> StructuredEncoder<List<T>> ofList(StructuredEncoder<T> encoder) {
		return (out, list) -> out.writeList(encoder, list);
	}
}
