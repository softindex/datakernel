package io.global.ot.util;

import io.datakernel.codec.Codecs;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.util.SimpleType;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.datakernel.util.Preconditions.checkNotNull;

public final class CodecRegistry implements CodecFactory {
	private final Map<Class<?>, BiFunction<CodecFactory, StructuredCodec<?>[], StructuredCodec<?>>> map = new HashMap<>();

	private CodecRegistry() {}

	public static CodecRegistry create() {
		return new CodecRegistry();
	}

	@SuppressWarnings("unchecked")
	public static CodecRegistry createDefault() {
		return create()
				.with(Void.class, Codecs.VOID_CODEC)
				.with(Boolean.class, Codecs.BOOLEAN_CODEC)
				.with(Character.class, Codecs.CHARACTER_CODEC)
				.with(Byte.class, Codecs.BYTE_CODEC)
				.with(Integer.class, Codecs.INT_CODEC)
				.with(Long.class, Codecs.LONG_CODEC)
				.with(Float.class, Codecs.FLOAT_CODEC)
				.with(Double.class, Codecs.DOUBLE_CODEC)
				.with(void.class, Codecs.VOID_CODEC)
				.with(boolean.class, Codecs.BOOLEAN_CODEC)
				.with(char.class, Codecs.CHARACTER_CODEC)
				.with(byte.class, Codecs.BYTE_CODEC)
				.with(int.class, Codecs.INT_CODEC)
				.with(long.class, Codecs.LONG_CODEC)
				.with(float.class, Codecs.FLOAT_CODEC)
				.with(double.class, Codecs.DOUBLE_CODEC)
				.with(String.class, Codecs.STRING_CODEC)
				.with(byte[].class, Codecs.BYTES_CODEC)
				.withGeneric(Optional.class, (registry, structuredCodecs) ->
						Codecs.ofOptional((StructuredCodec) structuredCodecs[0]))
				.withGeneric(List.class, (registry, structuredCodecs) ->
						Codecs.ofList((StructuredCodec) structuredCodecs[0]))
				.withGeneric(Set.class, (registry, structuredCodecs) ->
						Codecs.ofSet((StructuredCodec) structuredCodecs[0]))
				.withGeneric(Map.class, (registry, structuredCodecs) ->
						Codecs.ofMap((StructuredCodec) structuredCodecs[0]));
	}

	public <T> CodecRegistry with(Class<T> type, StructuredCodec<T> codec) {
		return withGeneric(type, (self, $) -> codec);
	}

	public <T> CodecRegistry with(Class<T> type, Function<CodecFactory, StructuredCodec<T>> codec) {
		return withGeneric(type, (self, $) -> codec.apply(self));
	}

	@SuppressWarnings("unchecked")
	public <T> CodecRegistry withGeneric(Class<T> type, BiFunction<CodecFactory, StructuredCodec<?>[], StructuredCodec<T>> fn) {
		map.put(type, (BiFunction) fn);
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> StructuredCodec<T> get(Type type) {
		return doGet(SimpleType.of(type));
	}

	@SuppressWarnings("unchecked")
	private <T> StructuredCodec<T> doGet(SimpleType type) {
		Class clazz = type.getRawType();
		BiFunction<CodecFactory, StructuredCodec<?>[], StructuredCodec<?>> fn = checkNotNull(map.get(clazz));

		StructuredCodec<?>[] subCodecs = new StructuredCodec[type.getTypeParams().length];

		SimpleType[] typeParams = type.getTypeParams();
		for (int i = 0; i < typeParams.length; i++) {
			subCodecs[i] = doGet(typeParams[i]);
		}

		return (StructuredCodec<T>) fn.apply(this, subCodecs);
	}

}
