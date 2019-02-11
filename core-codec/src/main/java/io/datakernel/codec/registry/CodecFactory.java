package io.datakernel.codec.registry;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.util.TypeT;

import java.lang.reflect.Type;

public interface CodecFactory {
	default <T> StructuredCodec<T> get(Class<T> type) {
		return get((Type) type);
	}

	default <T> StructuredCodec<T> get(TypeT<T> type) {
		return get(type.getType());
	}

	<T> StructuredCodec<T> get(Type type);
}
