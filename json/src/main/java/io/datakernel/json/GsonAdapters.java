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

package io.datakernel.json;

import com.google.gson.TypeAdapter;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import io.datakernel.exception.ParseException;
import io.datakernel.util.*;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;

import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;

@SuppressWarnings("unchecked")
public final class GsonAdapters {

	public static final TypeAdapter<Boolean> BOOLEAN_JSON = new TypeAdapter<Boolean>() {
		@Override
		public void write(JsonWriter out, Boolean value) throws IOException {
			out.value(value);
		}

		@Override
		public Boolean read(JsonReader in) throws IOException {
			return in.nextBoolean();
		}
	};

	public static final TypeAdapter<Character> CHARACTER_JSON = new TypeAdapter<Character>() {
		@Override
		public void write(JsonWriter out, Character value) throws IOException {
			out.value(value);
		}

		@Override
		public Character read(JsonReader in) throws IOException {
			String v = in.nextString();
			checkArgument(v.length() == 1, "Not a character");
			return v.charAt(0);
		}
	};

	public static final TypeAdapter<Byte> BYTE_JSON = new TypeAdapter<Byte>() {
		@Override
		public void write(JsonWriter out, Byte value) throws IOException {
			out.value(value);
		}

		@Override
		public Byte read(JsonReader in) throws IOException {
			int v = in.nextInt();
			checkArgument(v >= 0 && v <= 0xFF, "Not a byte");
			return (byte) v;
		}
	};

	public static final TypeAdapter<Short> SHORT_JSON = new TypeAdapter<Short>() {
		@Override
		public void write(JsonWriter out, Short value) throws IOException {
			out.value(value);
		}

		@Override
		public Short read(JsonReader in) throws IOException {
			int v = in.nextInt();
			checkArgument(v >= 0 && v <= 0xFFFF, "Not a short");
			return (short) in.nextInt();
		}
	};

	public static final TypeAdapter<Integer> INTEGER_JSON = new TypeAdapter<Integer>() {
		@Override
		public void write(JsonWriter out, Integer value) throws IOException {
			out.value(value);
		}

		@Override
		public Integer read(JsonReader in) throws IOException {
			return in.nextInt();
		}
	};

	public static final TypeAdapter<Long> LONG_JSON = new TypeAdapter<Long>() {
		@Override
		public void write(JsonWriter out, Long value) throws IOException {
			out.value(value);
		}

		@Override
		public Long read(JsonReader in) throws IOException {
			return in.nextLong();
		}
	};

	public static final TypeAdapter<Float> FLOAT_JSON = new TypeAdapter<Float>() {
		@Override
		public void write(JsonWriter out, Float value) throws IOException {
			out.value(value);
		}

		@Override
		public Float read(JsonReader in) throws IOException {
			return (float) in.nextDouble();
		}
	};

	public static final TypeAdapter<Double> DOUBLE_JSON = new TypeAdapter<Double>() {
		@Override
		public void write(JsonWriter out, Double value) throws IOException {
			out.value(value);
		}

		@Override
		public Double read(JsonReader in) throws IOException {
			return in.nextDouble();
		}
	};

	public static final TypeAdapter<String> STRING_JSON = new TypeAdapter<String>() {
		@Override
		public void write(JsonWriter out, String value) throws IOException {
			out.value(value);
		}

		@Override
		public String read(JsonReader in) throws IOException {
			return in.nextString();
		}
	};

	public static final TypeAdapter<byte[]> BYTES_JSON = GsonAdapters.transform(STRING_JSON,
			Base64.getDecoder()::decode,
			Base64.getEncoder()::encodeToString);

	public static final TypeAdapter<LocalDate> LOCAL_DATE_JSON = new TypeAdapter<LocalDate>() {
		@Override
		public void write(JsonWriter out, LocalDate value) throws IOException {
			out.value(value.toString());
		}

		@Override
		public LocalDate read(JsonReader in) throws IOException {
			return LocalDate.parse(in.nextString());
		}
	};

	public static final TypeAdapter<Class<Object>> CLASS_JSON = new TypeAdapter<Class<Object>>() {
		@Override
		public void write(JsonWriter out, Class<Object> value) throws IOException {
			out.value(value.getName());
		}

		@Override
		public Class<Object> read(JsonReader in) throws IOException {
			try {
				return (Class<Object>) Class.forName(in.nextString());
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}
	};

	public interface TypeAdapterMapping {

		<T> TypeAdapter<T> getAdapter(Type type);

		default <T> String toJson(T obj) {
			return GsonAdapters.toJson(getAdapter(obj.getClass()), obj);
		}
	}

	public static class TypeAdapterMappingImpl implements TypeAdapterMapping {

		@FunctionalInterface
		public interface AdapterSupplier {

			TypeAdapter<?> get(Class<?> cls, TypeAdapter<?>[] paramAdapters);
		}

		private final Map<Class<?>, AdapterSupplier> mapping = new HashMap<>();

		private TypeAdapterMappingImpl() {
		}

		public static TypeAdapterMappingImpl create() {
			return new TypeAdapterMappingImpl();
		}

		public static TypeAdapterMappingImpl from(TypeAdapterMappingImpl fallback) {
			TypeAdapterMappingImpl tam = new TypeAdapterMappingImpl();
			tam.mapping.putAll(fallback.mapping);
			return tam;
		}

		@Override
		public <T> TypeAdapter<T> getAdapter(Type type) {
			Class<T> cls;
			TypeAdapter<?>[] paramAdapters;
			if (type instanceof ParameterizedType) {
				ParameterizedType parameterized = (ParameterizedType) type;
				Type[] typeArgs = parameterized.getActualTypeArguments();
				cls = (Class<T>) parameterized.getRawType();
				paramAdapters = new TypeAdapter<?>[typeArgs.length];
				for (int i = 0; i < typeArgs.length; i++) {
					Type arg = typeArgs[i];
					checkState(arg != type, "Mapping does not support recurring generics!");
					paramAdapters[i] = getAdapter(arg);
				}
			} else {
				cls = (Class<T>) type;
				paramAdapters = new TypeAdapter<?>[0];
			}
			AdapterSupplier func = mapping.get(cls);
			if (func != null) {
				return (TypeAdapter<T>) func.get(cls, paramAdapters);
			}
			for (Map.Entry<Class<?>, AdapterSupplier> entry : mapping.entrySet()) {
				if (entry.getKey().isAssignableFrom(cls)) {
					return (TypeAdapter<T>) entry.getValue().get(cls, paramAdapters);
				}
			}
			throw new IllegalArgumentException("Type " + type.getTypeName() + " is not registered for this mapping!");
		}

		public TypeAdapterMappingImpl withAdapter(Class<?> type, TypeAdapter<?> adapter) {
			mapping.put(type, ($1, $2) -> adapter);
			return this;
		}

		public TypeAdapterMappingImpl withAdapter(Class<?> type, AdapterSupplier supplier) {
			mapping.put(type, supplier);
			return this;
		}
	}

	public static final TypeAdapterMappingImpl PRIMITIVES_MAP = TypeAdapterMappingImpl.create()
			.withAdapter(boolean.class, BOOLEAN_JSON).withAdapter(Boolean.class, BOOLEAN_JSON)
			.withAdapter(char.class, CHARACTER_JSON).withAdapter(Character.class, CHARACTER_JSON)
			.withAdapter(byte.class, BYTE_JSON).withAdapter(Byte.class, BYTE_JSON)
			.withAdapter(short.class, SHORT_JSON).withAdapter(Short.class, SHORT_JSON)
			.withAdapter(int.class, INTEGER_JSON).withAdapter(Integer.class, INTEGER_JSON)
			.withAdapter(long.class, LONG_JSON).withAdapter(Long.class, LONG_JSON)
			.withAdapter(float.class, FLOAT_JSON).withAdapter(Float.class, FLOAT_JSON)
			.withAdapter(double.class, DOUBLE_JSON).withAdapter(Double.class, DOUBLE_JSON)
			.withAdapter(String.class, STRING_JSON)
			.withAdapter(Enum.class, (cls, $) -> ofEnum((Class<Enum>) cls))
			.withAdapter(List.class, ($, paramAdapters) -> {
				checkArgument(paramAdapters.length == 1, "List must have 1 generic type parameter");
				return ofList(paramAdapters[0]);
			})
			.withAdapter(Set.class, ($, paramAdapters) -> {
				checkArgument(paramAdapters.length == 1, "Set must have 1 generic type parameter");
				return ofSet(paramAdapters[0]);
			})
			.withAdapter(Map.class, ($, paramAdapters) -> {
				checkArgument(paramAdapters.length == 2, "Map must have 2 generic type parameter");
				checkArgument(paramAdapters[0] == STRING_JSON, "Map key type should be string!");
				return ofMap(paramAdapters[1]);
			});

	// copied from GsonSubclassesAdapter
	// makes instantiation of stateless anonymous classes possible
	// yet still is a pretty bad thing to do
	static <T> T newInstance(String className) {
		try {
			Class<?> cls = Class.forName(className);
			boolean isStatic = (cls.getModifiers() & Modifier.STATIC) != 0;
			Class<?> enclosingClass = cls.getEnclosingClass();
			if (isStatic || enclosingClass == null) {
				Constructor<?> ctor = cls.getDeclaredConstructor();
				ctor.setAccessible(true);
				return (T) ctor.newInstance();
			}
			Constructor<?> ctor = cls.getDeclaredConstructor(enclosingClass);
			ctor.setAccessible(true);
			return (T) ctor.newInstance(new Object[]{null});
		} catch (Exception e) {
			throw new AssertionError(e);
		}
	}

	public static <T> TypeAdapter<T> stateless() {
		return new TypeAdapter<T>() {
			@Override
			public void write(JsonWriter out, T value) throws IOException {
				out.value(value.getClass().getName());
			}

			@Override
			public T read(JsonReader in) throws IOException {
				return newInstance(in.nextString());
			}
		};
	}

	public static <T> TypeAdapter<T> stateless(T... singletons) {
		Map<String, T> mapped = new HashMap<>();
		for (T singleton : singletons) {
			mapped.put(singleton.getClass().getName(), singleton);
		}
		return new TypeAdapter<T>() {
			@Override
			public void write(JsonWriter out, T value) throws IOException {
				out.value(value.getClass().getName());
			}

			@Override
			public T read(JsonReader in) throws IOException {
				String name = in.nextString();
				T singleton = mapped.get(name);
				if (singleton != null) {
					return singleton;
				}
				return newInstance(name);
			}
		};
	}

	public static <E extends Enum<E>> TypeAdapter<E> ofEnum(Class<E> enumType) {
		return new TypeAdapter<E>() {
			@Override
			public void write(JsonWriter out, E value) throws IOException {
				out.value(value.name());
			}

			@Override
			public E read(JsonReader in) throws IOException {
				return Enum.valueOf(enumType, in.nextString());
			}
		};
	}

/*
	public static TypeAdapter<byte[]> ofBytes() {
		return new TypeAdapter<byte[]>() {
			@Override
			public void write(JsonWriter out, byte[] value) throws IOException {
				out.value(Base64.getEncoder().encodeToString(value));
			}

			@Override
			public byte[] read(JsonReader in) throws IOException {
				String string = in.nextString();
				return Base64.getDecoder().decode(string);
			}
		};
	}
*/

	public static <I, O> TypeAdapter<O> transform(TypeAdapter<I> adapter, ParserFunction<I, O> from, Function<O, I> to) {
		return new TypeAdapter<O>() {
			@Override
			public void write(JsonWriter out, O value) {
				I result = to.apply(value);
				try {
					adapter.write(out, result);
				} catch (IOException e) {
					throw new AssertionError(e);
				}
			}

			@Override
			public O read(JsonReader in) throws IOException {
				I result = adapter.read(in);
				try {
					return from.parse(result);
				} catch (ParseException e) {
					throw new IOException(e);
				}
			}
		};
	}

	public static abstract class AbstractCollectionJson<C extends Collection<T>, T> extends TypeAdapter<C> {
		private final TypeAdapter<T> elementTypeAdapter;

		public AbstractCollectionJson(TypeAdapter<T> elementTypeAdapter) {
			this.elementTypeAdapter = elementTypeAdapter;
		}

		protected abstract C createCollection();

		@Override
		public C read(JsonReader in) throws IOException {
			C collection = createCollection();
			in.beginArray();
			while (in.hasNext()) {
				T instance = elementTypeAdapter.read(in);
				collection.add(instance);
			}
			in.endArray();
			return collection;
		}

		@Override
		public void write(JsonWriter out, C collection) throws IOException {
			out.beginArray();
			for (T element : collection) {
				elementTypeAdapter.write(out, element);
			}
			out.endArray();
		}
	}

	public static <T> TypeAdapter<List<T>> ofList(TypeAdapter<T> elementAdapter) {
		return new AbstractCollectionJson<List<T>, T>(elementAdapter) {
			@Override
			protected List<T> createCollection() {
				return new ArrayList<>();
			}
		};
	}

	public static <T> TypeAdapter<Set<T>> ofSet(TypeAdapter<T> elementAdapter) {
		return new AbstractCollectionJson<Set<T>, T>(elementAdapter) {
			@Override
			protected Set<T> createCollection() {
				return new LinkedHashSet<>();
			}
		};
	}

	public static <T> TypeAdapter<T[]> ofArray(TypeAdapter<T> elementAdapter) {
		return transform(ofList(elementAdapter), value -> (T[]) value.toArray(), Arrays::asList);
	}

	public static <V> TypeAdapter<Map<String, V>> ofMap(TypeAdapter<V> valueAdapter) {
		return new TypeAdapter<Map<String, V>>() {
			@Override
			public void write(JsonWriter out, Map<String, V> map) throws IOException {
				out.beginObject();
				for (Map.Entry<String, V> entry : map.entrySet()) {
					out.name(entry.getKey());
					valueAdapter.write(out, entry.getValue());
				}
				out.endObject();
			}

			@Override
			public Map<String, V> read(JsonReader in) throws IOException {
				Map<String, V> map = new LinkedHashMap<>();
				in.beginObject();
				while (in.hasNext()) {
					String key = in.nextName();
					V value = valueAdapter.read(in);
					map.put(key, value);
				}
				in.endObject();
				return map;
			}
		};
	}

	public static <K, V> TypeAdapter<Map<K, V>> ofMap(Function<K, String> to, Function<String, K> from, TypeAdapter<V> valueAdapter) {
		return new TypeAdapter<Map<K, V>>() {
			@Override
			public void write(JsonWriter out, Map<K, V> map) throws IOException {
				out.beginObject();
				for (Map.Entry<K, V> entry : map.entrySet()) {
					out.name(to.apply(entry.getKey()));
					valueAdapter.write(out, entry.getValue());
				}
				out.endObject();
			}

			@Override
			public Map<K, V> read(JsonReader in) throws IOException {
				Map<K, V> map = new LinkedHashMap<>();
				in.beginObject();
				while (in.hasNext()) {
					String key = in.nextName();
					V value = valueAdapter.read(in);
					map.put(from.apply(key), value);
				}
				in.endObject();
				return map;
			}
		};
	}

	public static TypeAdapter<Map<String, ?>> ofHeterogeneousMap(Map<String, ? extends TypeAdapter<?>> valueAdapters) {
		return new TypeAdapter<Map<String, ?>>() {
			@Override
			public void write(JsonWriter out, Map<String, ?> map) throws IOException {
				out.beginObject();
				for (Map.Entry<String, ?> entry : map.entrySet()) {
					String key = entry.getKey();
					TypeAdapter<Object> valueAdapter = (TypeAdapter<Object>) valueAdapters.get(key);
					out.name(key);
					valueAdapter.write(out, entry.getValue());
				}
				out.endObject();
			}

			@Override
			public Map<String, ?> read(JsonReader in) throws IOException {
				Map<String, Object> map = new LinkedHashMap<>();
				in.beginObject();
				while (in.hasNext()) {
					String key = in.nextName();
					TypeAdapter<Object> valueAdapter = (TypeAdapter<Object>) valueAdapters.get(key);
					Object value = valueAdapter.read(in);
					map.put(key, value);
				}
				in.endObject();
				return map;
			}
		};
	}

	public static <R, T1> TypeAdapter<R> ofTuple(TupleConstructor1<T1, R> constructor,
			String field1, Function<R, T1> getter1, TypeAdapter<T1> gson1) {
		return transform(ofHeterogeneousMap(map(field1, gson1)),
				map -> constructor.create((T1) map.get(field1)),
				obj -> map(field1, getter1.apply(obj)));
	}

	public static <R, T1, T2> TypeAdapter<R> ofTuple(TupleConstructor2<T1, T2, R> constructor,
			String field1, Function<R, T1> getter1, TypeAdapter<T1> gson1,
			String field2, Function<R, T2> getter2, TypeAdapter<T2> gson2) {
		return transform(ofHeterogeneousMap(map(field1, gson1, field2, gson2)),
				map -> constructor.create((T1) map.get(field1), (T2) map.get(field2)),
				obj -> map(field1, getter1.apply(obj), field2, getter2.apply(obj)));
	}

	public static <R, T1, T2, T3> TypeAdapter<R> ofTuple(TupleConstructor3<T1, T2, T3, R> constructor,
			String field1, Function<R, T1> getter1, TypeAdapter<T1> gson1,
			String field2, Function<R, T2> getter2, TypeAdapter<T2> gson2,
			String field3, Function<R, T3> getter3, TypeAdapter<T3> gson3) {
		return transform(ofHeterogeneousMap(map(field1, gson1, field2, gson2, field3, gson3)),
				map -> constructor.create((T1) map.get(field1), (T2) map.get(field2), (T3) map.get(field3)),
				obj -> map(field1, getter1.apply(obj), field2, getter2.apply(obj), field3, getter3.apply(obj)));
	}

	public static <R, T1, T2, T3, T4> TypeAdapter<R> ofTuple(TupleConstructor4<T1, T2, T3, T4, R> constructor,
			String field1, Function<R, T1> getter1, TypeAdapter<T1> gson1,
			String field2, Function<R, T2> getter2, TypeAdapter<T2> gson2,
			String field3, Function<R, T3> getter3, TypeAdapter<T3> gson3,
			String field4, Function<R, T4> getter4, TypeAdapter<T4> gson4) {
		return transform(ofHeterogeneousMap(map(field1, gson1, field2, gson2, field3, gson3, field4, gson4)),
				map -> constructor.create((T1) map.get(field1), (T2) map.get(field2), (T3) map.get(field3), (T4) map.get(field4)),
				obj -> map(field1, getter1.apply(obj), field2, getter2.apply(obj), field3, getter3.apply(obj), field4, getter4.apply(obj)));
	}

	public static <R, T1, T2, T3, T4, T5> TypeAdapter<R> ofTuple(TupleConstructor5<T1, T2, T3, T4, T5, R> constructor,
			String field1, Function<R, T1> getter1, TypeAdapter<T1> gson1,
			String field2, Function<R, T2> getter2, TypeAdapter<T2> gson2,
			String field3, Function<R, T3> getter3, TypeAdapter<T3> gson3,
			String field4, Function<R, T4> getter4, TypeAdapter<T4> gson4,
			String field5, Function<R, T5> getter5, TypeAdapter<T5> gson5) {
		return transform(ofHeterogeneousMap(map(field1, gson1, field2, gson2, field3, gson3, field4, gson4, field5, gson5)),
				map -> constructor.create((T1) map.get(field1), (T2) map.get(field2), (T3) map.get(field3), (T4) map.get(field4), (T5) map.get(field5)),
				obj -> map(field1, getter1.apply(obj), field2, getter2.apply(obj), field3, getter3.apply(obj), field4, getter4.apply(obj), field5, getter5.apply(obj)));
	}

	public static <R, T1, T2, T3, T4, T5, T6> TypeAdapter<R> ofTuple(TupleConstructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			String field1, Function<R, T1> getter1, TypeAdapter<T1> gson1,
			String field2, Function<R, T2> getter2, TypeAdapter<T2> gson2,
			String field3, Function<R, T3> getter3, TypeAdapter<T3> gson3,
			String field4, Function<R, T4> getter4, TypeAdapter<T4> gson4,
			String field5, Function<R, T5> getter5, TypeAdapter<T5> gson5,
			String field6, Function<R, T6> getter6, TypeAdapter<T6> gson6) {
		return transform(ofHeterogeneousMap(map(field1, gson1, field2, gson2, field3, gson3, field4, gson4, field5, gson5, field6, gson6)),
				map -> constructor.create((T1) map.get(field1), (T2) map.get(field2), (T3) map.get(field3), (T4) map.get(field4), (T5) map.get(field5), (T6) map.get(field6)),
				obj -> map(field1, getter1.apply(obj), field2, getter2.apply(obj), field3, getter3.apply(obj), field4, getter4.apply(obj), field5, getter5.apply(obj), field6, getter6.apply(obj)));
	}

	public static TypeAdapter<Object[]> ofHeterogeneousArray(TypeAdapter<?>[] valueAdapters) {
		return new TypeAdapter<Object[]>() {
			@Override
			public void write(JsonWriter writer, Object[] value) throws IOException {
				writer.beginArray();
				for (int i = 0; i < valueAdapters.length; i++) {
					TypeAdapter<Object> adapter = (TypeAdapter<Object>) valueAdapters[i];
					adapter.write(writer, value[i]);
				}
				writer.endArray();
			}

			@Override
			public Object[] read(JsonReader reader) throws IOException {
				reader.beginArray();
				Object[] result = new Object[valueAdapters.length];
				for (int i = 0; i < valueAdapters.length; i++) {
					TypeAdapter<?> adapter = valueAdapters[i];
					result[i] = adapter.read(reader);
				}
				reader.endArray();
				return result;
			}
		};
	}

	public static <T> TypeAdapter<Optional<T>> optional(TypeAdapter<T> adapter) {
		return new TypeAdapter<Optional<T>>() {
			@Override
			public void write(JsonWriter out, Optional<T> value) throws IOException {
				if (value.isPresent()) {
					adapter.write(out, value.get());
				} else {
					out.nullValue();
				}
			}

			@Override
			public Optional<T> read(JsonReader in) throws IOException {
				if (in.peek() != JsonToken.NULL) {
					return Optional.of(adapter.read(in));
				} else {
					in.nextNull();
					return Optional.empty();
				}
			}
		};
	}

	public static <T> TypeAdapter<T> oneline(TypeAdapter<T> adapter) {
		return indent(adapter, "");
	}

	public static <T> TypeAdapter<T> indent(TypeAdapter<T> adapter, String indent) {
		return new TypeAdapter<T>() {
			@Override
			public void write(JsonWriter out, T value) throws IOException {
				JsonWriterEx jsonWriterEx = (JsonWriterEx) out;
				String previousIndent = jsonWriterEx.getIndentEx();
				jsonWriterEx.setIndentEx(indent);
				if (indent.isEmpty()) {
					jsonWriterEx.out.write('\n');
				}
				adapter.write(out, value);
				jsonWriterEx.setIndentEx(previousIndent);
			}

			@Override
			public T read(JsonReader in) throws IOException {
				return adapter.read(in);
			}
		};
	}

	public static TypeAdapter<Object[]> ofHeterogeneousArray(List<? extends TypeAdapter<?>> valueAdapters) {
		return ofHeterogeneousArray(valueAdapters.toArray(new TypeAdapter<?>[0]));
	}

	public static TypeAdapter<List<?>> ofHeterogeneousList(TypeAdapter<?>[] valueAdapters) {
		return transform(ofHeterogeneousArray(valueAdapters), Arrays::asList, List::toArray);
	}

	public static TypeAdapter<List<?>> ofHeterogeneousList(List<? extends TypeAdapter<?>> valueAdapters) {
		return ofHeterogeneousList(valueAdapters.toArray(new TypeAdapter<?>[0]));
	}

	public static <T> T fromJson(TypeAdapter<T> typeAdapter, String string) throws ParseException {
		StringReader reader = new StringReader(string);
		JsonReader jsonReader = new JsonReader(reader);
		try {
			return typeAdapter.read(jsonReader);
		} catch (IOException e) {
			throw new ParseException(GsonAdapters.class, "Failed to read value from JSON", e);
		}
	}

	public static final class JsonWriterEx extends JsonWriter {
		final Writer out;

		public JsonWriterEx(Writer out) {
			super(out);
			this.out = out;
		}

		private String indentEx;

		public final void setIndentEx(String indent) {
			this.indentEx = indent;
			setIndent(indent);
		}

		@Override
		public JsonWriter name(String name) throws IOException {
			return super.name(name);
		}

		public final String getIndentEx() {
			return indentEx;
		}
	}

	private static <T> void toJson(TypeAdapter<T> adapter, T value, Writer writer) throws IOException {
		JsonWriterEx jsonWriter = new JsonWriterEx(writer);
		jsonWriter.setLenient(true);
		jsonWriter.setIndentEx("");
		jsonWriter.setHtmlSafe(false);
		jsonWriter.setSerializeNulls(false);
		adapter.write(jsonWriter, value);
	}

	public static <T> String toJson(TypeAdapter<? super T> adapter, T value) {
		try {
			StringWriter writer = new StringWriter();
			toJson(adapter, value, writer);
			return writer.toString();
		} catch (IOException e) {
			throw new AssertionError(e); // no I/O with StringWriter
		}
	}

	public static <T> void toJson(TypeAdapter<? super T> adapter, T obj, Appendable appendable) throws IOException {
		toJson(adapter, obj, Streams.writerForAppendable(appendable));
	}
}
