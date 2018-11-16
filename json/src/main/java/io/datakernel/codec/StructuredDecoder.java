package io.datakernel.codec;

import io.datakernel.exception.ParseException;

public interface StructuredDecoder<T> {
	T decode(StructuredInput in) throws ParseException;
}
