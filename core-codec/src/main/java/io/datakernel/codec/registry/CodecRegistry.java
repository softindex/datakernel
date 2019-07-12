/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.codec.registry;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.util.*;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * A registry which stores codecs by their type and allows dynamic dispatch of them.
 * <p>
 * Also it allows dynamic construction of codecs for generic types.
 */
public final class CodecRegistry implements CodecFactory {
	private final Map<Class<?>, BiFunction<CodecFactory, StructuredCodec<?>[], StructuredCodec<?>>> map = new HashMap<>();

	private CodecRegistry() {
	}

	/**
	 * Creates a new completely empty registry.
	 * You are advised to use {@link #createDefault()} factory method instead.
	 */
	public static CodecRegistry create() {
		return new CodecRegistry();
	}

	/**
	 * Creates a registry with a set of default codcecs - primitives, some Java types, collections, DataKernel tuples.
	 */
	public static CodecRegistry createDefault() {
		return create()
				.with(void.class, VOID_CODEC)
				.with(boolean.class, BOOLEAN_CODEC)
				.with(char.class, CHARACTER_CODEC)
				.with(byte.class, BYTE_CODEC)
				.with(int.class, INT_CODEC)
				.with(long.class, LONG_CODEC)
				.with(float.class, FLOAT_CODEC)
				.with(double.class, DOUBLE_CODEC)

				.with(Void.class, VOID_CODEC)
				.with(Boolean.class, BOOLEAN_CODEC)
				.with(Character.class, CHARACTER_CODEC)
				.with(Byte.class, BYTE_CODEC)
				.with(Integer.class, INT_CODEC)
				.with(Long.class, LONG_CODEC)
				.with(Float.class, FLOAT_CODEC)
				.with(Double.class, DOUBLE_CODEC)

				.with(String.class, STRING_CODEC)

				.with(byte[].class, BYTES_CODEC)

				.withGeneric(Optional.class, (registry, subCodecs) ->
						ofOptional(subCodecs[0]))

				.withGeneric(Set.class, (registry, subCodecs) ->
						ofSet(subCodecs[0]))
				.withGeneric(List.class, (registry, subCodecs) ->
						ofList(subCodecs[0]))
				.withGeneric(Map.class, (registry, subCodecs) ->
						ofMap(subCodecs[0], subCodecs[1]))

				.withGeneric(Tuple1.class, (registry, subCodecs) ->
						tuple(Tuple1::new,
								Tuple1::getValue1, subCodecs[0]))
				.withGeneric(Tuple2.class, (registry, subCodecs) ->
						tuple(Tuple2::new,
								Tuple2::getValue1, subCodecs[0],
								Tuple2::getValue2, subCodecs[1]))
				.withGeneric(Tuple3.class, (registry, subCodecs) ->
						tuple(Tuple3::new,
								Tuple3::getValue1, subCodecs[0],
								Tuple3::getValue2, subCodecs[1],
								Tuple3::getValue3, subCodecs[2]))
				.withGeneric(Tuple4.class, (registry, subCodecs) ->
						tuple(Tuple4::new,
								Tuple4::getValue1, subCodecs[0],
								Tuple4::getValue2, subCodecs[1],
								Tuple4::getValue3, subCodecs[2],
								Tuple4::getValue4, subCodecs[3]))
				.withGeneric(Tuple5.class, (registry, subCodecs) ->
						tuple(Tuple5::new,
								Tuple5::getValue1, subCodecs[0],
								Tuple5::getValue2, subCodecs[1],
								Tuple5::getValue3, subCodecs[2],
								Tuple5::getValue4, subCodecs[3],
								Tuple5::getValue5, subCodecs[4]))
				.withGeneric(Tuple6.class, (registry, subCodecs) ->
						tuple(Tuple6::new,
								Tuple6::getValue1, subCodecs[0],
								Tuple6::getValue2, subCodecs[1],
								Tuple6::getValue3, subCodecs[2],
								Tuple6::getValue4, subCodecs[3],
								Tuple6::getValue5, subCodecs[4],
								Tuple6::getValue6, subCodecs[5]))
				;
	}

	public <T> CodecRegistry with(Class<T> type, StructuredCodec<T> codec) {
		return withGeneric(type, (self, $) -> codec);
	}

	public <T> CodecRegistry with(Class<T> type, Function<CodecFactory, StructuredCodec<T>> codec) {
		return withGeneric(type, (self, $) -> codec.apply(self));
	}

	@SuppressWarnings("unchecked")
	public <T> CodecRegistry withGeneric(Class<T> type, BiFunction<CodecFactory, StructuredCodec<Object>[], StructuredCodec<? extends T>> fn) {
		map.put(type, (BiFunction) fn);
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> StructuredCodec<T> get(Type type) {
		if (type instanceof Class && (Enum.class.isAssignableFrom((Class) type))) {
			return ofEnum((Class) type);
		}
		return doGet(RecursiveType.of(type));
	}

	@SuppressWarnings("unchecked")
	private <T> StructuredCodec<T> doGet(RecursiveType type) {
		Class clazz = type.getRawType();
		BiFunction<CodecFactory, StructuredCodec<?>[], StructuredCodec<?>> fn = checkNotNull(map.get(clazz));

		StructuredCodec<Object>[] subCodecs = new StructuredCodec[type.getTypeParams().length];

		RecursiveType[] typeParams = type.getTypeParams();
		for (int i = 0; i < typeParams.length; i++) {
			subCodecs[i] = doGet(typeParams[i]);
		}

		return (StructuredCodec<T>) fn.apply(this, subCodecs);
	}
}
