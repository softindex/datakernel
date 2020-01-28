package io.datakernel.codec;

import io.datakernel.common.parse.ParseException;

import java.util.function.Supplier;

/**
 * Encorer can read an object of type T from a {@link StructuredInput}.
 */
public interface StructuredDecoder<T> {
	T decode(StructuredInput in) throws ParseException;

	static <T> StructuredDecoder<T> ofObject(StructuredDecoder<T> decoder) {
		return in -> in.readObject(decoder);
	}

	static <T> StructuredDecoder<T> ofObject(Supplier<T> supplier) {
		return ofObject(in -> supplier.get());
	}

	static <T> StructuredDecoder<T> ofTuple(StructuredDecoder<T> decoder) {
		return in -> in.readTuple(decoder);
	}
}
