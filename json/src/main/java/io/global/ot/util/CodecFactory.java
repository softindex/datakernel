package io.global.ot.util;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredEncoder;
import io.datakernel.util.SimpleType;

import java.lang.reflect.Type;

public interface CodecFactory {
	default <T> StructuredCodec<T> get(Class<T> type) {
		return get(SimpleType.of(type));
	}

	default <T> StructuredCodec<T> get(Class<?> type, Class<?> subtypes1, Class<?>... subtypes) {
		return get(SimpleType.of(type, subtypes1, subtypes));
	}

	default <T> StructuredEncoder<T> get(Type type) {
		return get(SimpleType.ofType(type));
	}

	<T> StructuredCodec<T> get(SimpleType type);
}
