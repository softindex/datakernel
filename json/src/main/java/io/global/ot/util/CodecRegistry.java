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

package io.global.ot.util;

import io.datakernel.codec.Codecs;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.util.*;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.datakernel.codec.Codecs.concat;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public final class CodecRegistry implements CodecFactory {
	private final Map<Class<?>, BiFunction<CodecFactory, StructuredCodec<?>[], StructuredCodec<?>>> map = new HashMap<>();

	private CodecRegistry() {
	}

	public static CodecRegistry create() {
		return new CodecRegistry();
	}

	@SuppressWarnings("unchecked")
	public static CodecRegistry createDefault() {
		return create()
				.with(void.class, Codecs.VOID_CODEC)
				.with(boolean.class, Codecs.BOOLEAN_CODEC)
				.with(char.class, Codecs.CHARACTER_CODEC)
				.with(byte.class, Codecs.BYTE_CODEC)
				.with(int.class, Codecs.INT_CODEC)
				.with(long.class, Codecs.LONG_CODEC)
				.with(float.class, Codecs.FLOAT_CODEC)
				.with(double.class, Codecs.DOUBLE_CODEC)

				.with(Void.class, Codecs.VOID_CODEC)
				.with(Boolean.class, Codecs.BOOLEAN_CODEC)
				.with(Character.class, Codecs.CHARACTER_CODEC)
				.with(Byte.class, Codecs.BYTE_CODEC)
				.with(Integer.class, Codecs.INT_CODEC)
				.with(Long.class, Codecs.LONG_CODEC)
				.with(Float.class, Codecs.FLOAT_CODEC)
				.with(Double.class, Codecs.DOUBLE_CODEC)

				.with(String.class, Codecs.STRING_CODEC)

				.with(byte[].class, Codecs.BYTES_CODEC) // other primitive arrays?

				.withGeneric(Optional.class, (registry, structuredCodecs) ->
						Codecs.ofOptional((StructuredCodec) structuredCodecs[0]))

				.withGeneric(Set.class, (registry, structuredCodecs) ->
						Codecs.ofSet((StructuredCodec) structuredCodecs[0]))
				.withGeneric(List.class, (registry, structuredCodecs) ->
						Codecs.ofList((StructuredCodec) structuredCodecs[0]))
				.withGeneric(Map.class, (registry, structuredCodecs) ->
						Codecs.ofMap((StructuredCodec) structuredCodecs[0]))

				.withGeneric(Tuple1.class, (registry, structuredCodecs) ->
						(StructuredCodec) structuredCodecs[0])
				.withGeneric(Tuple2.class, (registry, structuredCodecs) ->
						concat(structuredCodecs)
								.transform(list -> new Tuple2(list.get(0), list.get(1)),
										tuple -> asList(tuple.getValue1(), tuple.getValue2())))
				.withGeneric(Tuple3.class, (registry, structuredCodecs) ->
						concat(structuredCodecs)
								.transform(list -> new Tuple3(list.get(0), list.get(1), list.get(2)),
										tuple -> asList(tuple.getValue1(), tuple.getValue2(), tuple.getValue3())))
				.withGeneric(Tuple4.class, (registry, structuredCodecs) ->
						concat(structuredCodecs)
								.transform(list -> new Tuple4(list.get(0), list.get(1), list.get(2), list.get(3)),
										tuple -> asList(tuple.getValue1(), tuple.getValue2(), tuple.getValue3(), tuple.getValue4())))
				.withGeneric(Tuple5.class, (registry, structuredCodecs) ->
						concat(structuredCodecs)
								.transform(list -> new Tuple5(list.get(0), list.get(1), list.get(2), list.get(3), list.get(4)),
										tuple -> asList(tuple.getValue1(), tuple.getValue2(), tuple.getValue3(), tuple.getValue4(), tuple.getValue5())))
				.withGeneric(Tuple6.class, (registry, structuredCodecs) ->
						concat(structuredCodecs)
								.transform(list -> new Tuple6(list.get(0), list.get(1), list.get(2), list.get(3), list.get(4), list.get(5)),
										tuple -> asList(tuple.getValue1(), tuple.getValue2(), tuple.getValue3(), tuple.getValue4(), tuple.getValue5(), tuple.getValue6())));
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
		return doGet(RecursiveType.of(type));
	}

	@SuppressWarnings("unchecked")
	private <T> StructuredCodec<T> doGet(RecursiveType type) {
		Class clazz = type.getRawType();
		BiFunction<CodecFactory, StructuredCodec<?>[], StructuredCodec<?>> fn = checkNotNull(map.get(clazz));

		StructuredCodec<?>[] subCodecs = new StructuredCodec[type.getTypeParams().length];

		RecursiveType[] typeParams = type.getTypeParams();
		for (int i = 0; i < typeParams.length; i++) {
			subCodecs[i] = doGet(typeParams[i]);
		}

		return (StructuredCodec<T>) fn.apply(this, subCodecs);
	}

}
