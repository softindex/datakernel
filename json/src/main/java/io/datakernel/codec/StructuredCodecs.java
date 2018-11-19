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

package io.datakernel.codec;

import io.datakernel.exception.ParseException;
import io.datakernel.util.*;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Function;

import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@SuppressWarnings("unchecked")
public final class StructuredCodecs {

	public static final StructuredCodec<Boolean> BOOLEAN_CODEC = new StructuredCodec<Boolean>() {
		@Override
		public void encode(StructuredOutput out, Boolean value) {
			out.writeBoolean(value);
		}

		@Override
		public Boolean decode(StructuredInput in) throws ParseException {
			return in.readBoolean();
		}
	};

	public static final StructuredCodec<Character> CHARACTER_CODEC = new StructuredCodec<Character>() {
		@Override
		public void encode(StructuredOutput out, Character value) {
			out.writeString(value + "");
		}

		@Override
		public Character decode(StructuredInput in) throws ParseException {
			String v = in.readString();
			if (v.length() == 1) return v.charAt(0);
			throw new ParseException();
		}
	};

	public static final StructuredCodec<Byte> BYTE_CODEC = new StructuredCodec<Byte>() {
		@Override
		public void encode(StructuredOutput out, Byte value) {
			out.writeInt(value & 0xFF);
		}

		@Override
		public Byte decode(StructuredInput in) throws ParseException {
			int v = in.readInt();
			if (v >= 0 && v <= 0xFF) return (byte) v;
			throw new ParseException();
		}
	};

	public static final StructuredCodec<Integer> INT_CODEC = new StructuredCodec<Integer>() {
		@Override
		public void encode(StructuredOutput out, Integer value) {
			out.writeInt(value);
		}

		@Override
		public Integer decode(StructuredInput in) throws ParseException {
			return in.readInt();
		}
	};

	public static final StructuredCodec<Long> LONG_CODEC = new StructuredCodec<Long>() {
		@Override
		public void encode(StructuredOutput out, Long value) {
			out.writeLong(value);
		}

		@Override
		public Long decode(StructuredInput in) throws ParseException {
			return in.readLong();
		}
	};

	public static final StructuredCodec<Integer> INT32_CODEC = new StructuredCodec<Integer>() {
		@Override
		public void encode(StructuredOutput out, Integer value) {
			out.writeInt32(value);
		}

		@Override
		public Integer decode(StructuredInput in) throws ParseException {
			return in.readInt32();
		}
	};

	public static final StructuredCodec<Long> LONG64_CODEC = new StructuredCodec<Long>() {
		@Override
		public void encode(StructuredOutput out, Long value) {
			out.writeLong64(value);
		}

		@Override
		public Long decode(StructuredInput in) throws ParseException {
			return in.readLong64();
		}
	};

	public static final StructuredCodec<Float> FLOAT_CODEC = new StructuredCodec<Float>() {
		@Override
		public void encode(StructuredOutput out, Float value) {
			out.writeFloat(value);
		}

		@Override
		public Float decode(StructuredInput in) throws ParseException {
			return in.readFloat();
		}
	};

	public static final StructuredCodec<Double> DOUBLE_CODEC = new StructuredCodec<Double>() {
		@Override
		public void encode(StructuredOutput out, Double value) {
			out.writeDouble(value);
		}

		@Override
		public Double decode(StructuredInput in) throws ParseException {
			return in.readDouble();
		}
	};

	public static final StructuredCodec<String> STRING_CODEC = new StructuredCodec<String>() {
		@Override
		public void encode(StructuredOutput out, String value) {
			out.writeString(value);
		}

		@Override
		public String decode(StructuredInput in) throws ParseException {
			return in.readString();
		}
	};

	public static final StructuredCodec<byte[]> BYTES_CODEC = new StructuredCodec<byte[]>() {
		@Override
		public void encode(StructuredOutput out, byte[] value) {
			out.writeBytes(value);
		}

		@Override
		public byte[] decode(StructuredInput in) throws ParseException {
			return in.readBytes();
		}
	};

	public static final StructuredCodec<Void> VOID_CODEC = new StructuredCodec<Void>() {
		@Override
		public Void decode(StructuredInput in) throws ParseException {
			return in.readNull();
		}

		@Override
		public void encode(StructuredOutput out, Void item) {
			out.writeNull();
		}
	};

	static <T> StructuredCodec<T> ofCustomType(Class<T> type) {
		return ofCustomType((Type) type);
	}

	static <T> StructuredCodec<T> ofCustomType(Type type) {
		return new StructuredCodec<T>() {
			@Override
			public void encode(StructuredOutput out, T item) {
				out.writeCustom(type, item);
			}

			@Override
			public T decode(StructuredInput in) throws ParseException {
				return in.readCustom(type);
			}
		};
	}

	public static <T> StructuredCodec<Optional<T>> ofOptional(StructuredCodec<T> codec) {
		return new StructuredCodec<Optional<T>>() {
			@Override
			public Optional<T> decode(StructuredInput in) throws ParseException {
				return Optional.ofNullable(in.readNullable(codec));
			}

			@Override
			public void encode(StructuredOutput out, Optional<T> item) {
				out.writeNullable(codec, item.orElse(null));
			}
		};
	}

	public static <T> StructuredCodec<List<T>> ofList(StructuredCodec<T> valueAdapters) {
		return new StructuredCodec<List<T>>() {
			@Override
			public List<T> decode(StructuredInput in) throws ParseException {
				return in.readList(valueAdapters);
			}

			@Override
			public void encode(StructuredOutput out, List<T> item) {
				out.writeList(valueAdapters, item);
			}
		};
	}

	public static <T> StructuredCodec<Set<T>> ofSet(StructuredCodec<T> codec) {
		return ofList(codec)
				.transform(LinkedHashSet::new, ArrayList::new);
	}

	public static <T> StructuredCodec<List<T>> ofListEx(StructuredCodec<? extends T>... elementDecoders) {
		return ofListEx(asList(elementDecoders));
	}

	public static <T> StructuredCodec<List<T>> ofListEx(List<StructuredCodec<? extends T>> codecs) {
		return new StructuredCodec<List<T>>() {
			@Override
			public List<T> decode(StructuredInput in) throws ParseException {
				List<T> list = new ArrayList<>();
				StructuredInput.ListReader listReader = in.listReader(false);
				for (StructuredCodec<? extends T> codec : codecs) {
					list.add(codec.decode(listReader.next()));
				}
				listReader.close();
				return list;
			}

			@Override
			public void encode(StructuredOutput out, List<T> list) {
				checkArgument(list.size() == codecs.size());
				Iterator<T> it = list.iterator();
				StructuredOutput.ListWriter listWriter = out.listWriter(false);
				for (StructuredCodec<? extends T> codec : codecs) {
					((StructuredCodec<T>) codec).encode(listWriter.next(), it.next());
				}
				listWriter.close();
			}
		};
	}

	public static <K, V> StructuredCodec<Map<K, V>> ofMap(StructuredCodec<K> codecKey, StructuredCodec<V> codecValue) {
		return StructuredCodecs.<Tuple2<K, V>>ofList(
				record(Tuple2::new,
						Tuple2::getValue1, codecKey,
						Tuple2::getValue2, codecValue))
				.transform(
						tuples -> tuples.stream().collect(toMap(Tuple2::getValue1, Tuple2::getValue2)),
						map -> map.entrySet().stream().map(entry -> new Tuple2<>(entry.getKey(), entry.getValue())).collect(toList()));
	}

	public static <T> StructuredCodec<Map<String, T>> ofMap(StructuredCodec<T> codec) {
		return new StructuredCodec<Map<String, T>>() {
			@Override
			public void encode(StructuredOutput out, Map<String, T> map) {
				out.writeMap(codec, map);
			}

			@Override
			public Map<String, T> decode(StructuredInput in) throws ParseException {
				return in.readMap(codec);
			}
		};
	}

	public static <T> StructuredCodec<Map<String, T>> ofMapEx(Map<String, StructuredCodec<? extends T>> fieldCodecs) {
		return new StructuredCodec<Map<String, T>>() {
			@Override
			public Map<String, T> decode(StructuredInput in) throws ParseException {
				Map<String, T> map = new LinkedHashMap<>();
				StructuredInput.MapReader mapReader = in.mapReader(false);
				for (Map.Entry<String, StructuredCodec<? extends T>> entry : fieldCodecs.entrySet()) {
					String field = entry.getKey();
					StructuredCodec<? extends T> codec = entry.getValue();
					Tuple2<String, StructuredInput> next = mapReader.next();
					String actualField = next.getValue1();
					if (actualField != null && !field.equals(actualField)) {
						throw new ParseException("Field expected: " + field + ", actual: " + actualField);
					}
					map.put(field, codec.decode(next.getValue2()));
				}
				mapReader.close();
				return map;
			}

			@Override
			public void encode(StructuredOutput out, Map<String, T> map) {
				StructuredOutput.MapWriter mapWriter = out.mapWriter(false);
				for (Map.Entry<String, StructuredCodec<? extends T>> entry : fieldCodecs.entrySet()) {
					String field = entry.getKey();
					StructuredCodec<T> codec = (StructuredCodec<T>) entry.getValue();
					codec.encode(mapWriter.next(field), map.get(field));
				}
				mapWriter.close();
			}
		};
	}

	public static <T> StructuredCodec<List<T>> concat(StructuredCodec<? extends T>... elementCodecs) {
		return concat(asList(elementCodecs));
	}

	public static <T> StructuredCodec<List<T>> concat(List<StructuredCodec<? extends T>> elementCodecs) {
		return new StructuredCodec<List<T>>() {
			@Override
			public List<T> decode(StructuredInput in) throws ParseException {
				List<T> result = new ArrayList<>(elementCodecs.size());
				for (StructuredCodec<? extends T> elementCodec : elementCodecs) {
					result.add(elementCodec.decode(in));
				}
				return result;
			}

			@Override
			public void encode(StructuredOutput out, List<T> item) {
				checkArgument(item.size() == elementCodecs.size());
				for (int i = 0; i < elementCodecs.size(); i++) {
					((StructuredCodec<T>) elementCodecs.get(i)).encode(out, item.get(i));
				}
			}
		};
	}

	public static <R> StructuredCodec<R> record(TupleParser0<R> constructor) {
		return ofListEx()
				.transform(
						list -> constructor.create(),
						item -> emptyList());
	}

	public static <R, T1> StructuredCodec<R> record(TupleParser1<T1, R> constructor,
			Function<R, T1> getter1, StructuredCodec<T1> codec1) {
		return ofListEx(codec1)
				.transform(
						list -> constructor.create((T1) list.get(0)),
						item -> singletonList(getter1.apply(item)));
	}

	public static <R, T1, T2> StructuredCodec<R> record(TupleParser2<T1, T2, R> constructor,
			Function<R, T1> getter1, StructuredCodec<T1> codec1,
			Function<R, T2> getter2, StructuredCodec<T2> codec2) {
		return ofListEx(codec1, codec2)
				.transform(
						list -> constructor.create((T1) list.get(0), (T2) list.get(1)),
						item -> asList(getter1.apply(item), getter2.apply(item)));
	}

	public static <R, T1, T2, T3> StructuredCodec<R> record(TupleParser3<T1, T2, T3, R> constructor,
			Function<R, T1> getter1, StructuredCodec<T1> codec1,
			Function<R, T2> getter2, StructuredCodec<T2> codec2,
			Function<R, T3> getter3, StructuredCodec<T3> codec3) {
		return ofListEx(codec1, codec2, codec3)
				.transform(
						list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)),
						item -> asList(getter1.apply(item), getter2.apply(item), getter3.apply(item)));
	}

	public static <R, T1, T2, T3, T4> StructuredCodec<R> record(TupleParser4<T1, T2, T3, T4, R> constructor,
			Function<R, T1> getter1, StructuredCodec<T1> codec1,
			Function<R, T2> getter2, StructuredCodec<T2> codec2,
			Function<R, T3> getter3, StructuredCodec<T3> codec3,
			Function<R, T4> getter4, StructuredCodec<T4> codec4) {
		return ofListEx(codec1, codec2, codec3, codec4)
				.transform(
						list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3)),
						item -> asList(getter1.apply(item), getter2.apply(item), getter3.apply(item), getter4.apply(item)));
	}

	public static <R, T1, T2, T3, T4, T5> StructuredCodec<R> record(TupleParser5<T1, T2, T3, T4, T5, R> constructor,
			Function<R, T1> getter1, StructuredCodec<T1> codec1,
			Function<R, T2> getter2, StructuredCodec<T2> codec2,
			Function<R, T3> getter3, StructuredCodec<T3> codec3,
			Function<R, T4> getter4, StructuredCodec<T4> codec4,
			Function<R, T5> getter5, StructuredCodec<T5> codec5) {
		return ofListEx(codec1, codec2, codec3, codec4, codec5)
				.transform(
						list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4)),
						item -> asList(getter1.apply(item), getter2.apply(item), getter3.apply(item), getter4.apply(item), getter5.apply(item)));
	}

	public static <R, T1, T2, T3, T4, T5, T6> StructuredCodec<R> record(TupleParser6<T1, T2, T3, T4, T5, T6, R> constructor,
			Function<R, T1> getter1, StructuredCodec<T1> codec1,
			Function<R, T2> getter2, StructuredCodec<T2> codec2,
			Function<R, T3> getter3, StructuredCodec<T3> codec3,
			Function<R, T4> getter4, StructuredCodec<T4> codec4,
			Function<R, T5> getter5, StructuredCodec<T5> codec5,
			Function<R, T6> getter6, StructuredCodec<T6> codec6) {
		return ofListEx(codec1, codec2, codec3, codec4, codec5, codec6)
				.transform(
						list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4), (T6) list.get(5)),
						item -> asList(getter1.apply(item), getter2.apply(item), getter3.apply(item), getter4.apply(item), getter5.apply(item), getter6.apply(item)));
	}

	public static <R, T1> StructuredCodec<R> recordAsList(TupleParser1<T1, R> constructor,
			Function<R, T1> getter1, StructuredCodec<T1> codec1) {
		return ofListEx(codec1)
				.transform(
						list -> constructor.create((T1) list.get(0)),
						item -> asList(getter1.apply(item)));
	}

	public static <R> StructuredCodec<R> recordAsMap(TupleParser0<R> constructor) {
		return ofMapEx(emptyMap())
				.transform(
						map -> constructor.create(),
						item -> map());
	}

	public static <R, T1> StructuredCodec<R> recordAsMap(TupleParser1<T1, R> constructor,
			String field1, Function<R, T1> getter1, StructuredCodec<T1> codec1) {
		return ofMapEx(map(field1, codec1))
				.transform(
						map -> constructor.create((T1) map.get(field1)),
						item -> map(field1, getter1.apply(item)));
	}

	public static <R, T1, T2> StructuredCodec<R> recordAsMap(TupleParser2<T1, T2, R> constructor,
			String field1, Function<R, T1> getter1, StructuredCodec<T1> codec1,
			String field2, Function<R, T2> getter2, StructuredCodec<T2> codec2) {
		return ofMapEx(map(field1, codec1, field2, codec2))
				.transform(
						map -> constructor.create((T1) map.get(field1), (T2) map.get(field2)),
						item -> map(field1, getter1.apply(item), field2, getter2.apply(item)));
	}

	public static <R, T1, T2, T3> StructuredCodec<R> recordAsMap(TupleParser3<T1, T2, T3, R> constructor,
			String field1, Function<R, T1> getter1, StructuredCodec<T1> codec1,
			String field2, Function<R, T2> getter2, StructuredCodec<T2> codec2,
			String field3, Function<R, T3> getter3, StructuredCodec<T3> codec3) {
		return ofMapEx(map(field1, codec1, field2, codec2, field3, codec3))
				.transform(
						map -> constructor.create((T1) map.get(field1), (T2) map.get(field2), (T3) map.get(field3)),
						item -> map(field1, getter1.apply(item), field2, getter2.apply(item), field3, getter3.apply(item)));
	}

	public static <R, T1, T2, T3, T4> StructuredCodec<R> recordAsMap(TupleParser4<T1, T2, T3, T4, R> constructor,
			String field1, Function<R, T1> getter1, StructuredCodec<T1> codec1,
			String field2, Function<R, T2> getter2, StructuredCodec<T2> codec2,
			String field3, Function<R, T3> getter3, StructuredCodec<T3> codec3,
			String field4, Function<R, T4> getter4, StructuredCodec<T4> codec4) {
		return ofMapEx(map(field1, codec1, field2, codec2, field3, codec3, field4, codec4))
				.transform(
						map -> constructor.create((T1) map.get(field1), (T2) map.get(field2), (T3) map.get(field3), (T4) map.get(field4)),
						item -> map(field1, getter1.apply(item), field2, getter2.apply(item), field3, getter3.apply(item), field4, getter4.apply(item)));
	}

	public static <R, T1, T2, T3, T4, T5> StructuredCodec<R> recordAsMap(TupleParser5<T1, T2, T3, T4, T5, R> constructor,
			String field1, Function<R, T1> getter1, StructuredCodec<T1> codec1,
			String field2, Function<R, T2> getter2, StructuredCodec<T2> codec2,
			String field3, Function<R, T3> getter3, StructuredCodec<T3> codec3,
			String field4, Function<R, T4> getter4, StructuredCodec<T4> codec4,
			String field5, Function<R, T5> getter5, StructuredCodec<T5> codec5) {
		return ofMapEx(map(field1, codec1, field2, codec2, field3, codec3, field4, codec4, field5, codec5))
				.transform(
						map -> constructor.create((T1) map.get(field1), (T2) map.get(field2), (T3) map.get(field3), (T4) map.get(field4), (T5) map.get(field5)),
						item -> map(field1, getter1.apply(item), field2, getter2.apply(item), field3, getter3.apply(item), field4, getter4.apply(item), field5, getter5.apply(item)));
	}

	public static <R, T1, T2, T3, T4, T5, T6> StructuredCodec<R> recordAsMap(TupleParser6<T1, T2, T3, T4, T5, T6, R> constructor,
			String field1, Function<R, T1> getter1, StructuredCodec<T1> codec1,
			String field2, Function<R, T2> getter2, StructuredCodec<T2> codec2,
			String field3, Function<R, T3> getter3, StructuredCodec<T3> codec3,
			String field4, Function<R, T4> getter4, StructuredCodec<T4> codec4,
			String field5, Function<R, T5> getter5, StructuredCodec<T5> codec5,
			String field6, Function<R, T6> getter6, StructuredCodec<T6> codec6) {
		return ofMapEx(map(field1, codec1, field2, codec2, field3, codec3, field4, codec4, field5, codec5, field6, codec6))
				.transform(
						map -> constructor.create((T1) map.get(field1), (T2) map.get(field2), (T3) map.get(field3), (T4) map.get(field4), (T5) map.get(field5), (T6) map.get(field6)),
						item -> map(field1, getter1.apply(item), field2, getter2.apply(item), field3, getter3.apply(item), field4, getter4.apply(item), field5, getter5.apply(item), field6, getter6.apply(item)));
	}
}
