package io.datakernel.utils;

import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import org.joda.time.LocalDate;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Function;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

public class GsonAdapters {

	public static final TypeAdapter<Byte> BYTE_JSON = new TypeAdapter<Byte>() {
		@Override
		public void write(JsonWriter out, Byte value) throws IOException {
			out.value(value);
		}

		@Override
		public Byte read(JsonReader in) throws IOException {
			int v = in.nextInt();
			checkArgument(v >= 0 && v <= 0xFF);
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
			checkArgument(v >= 0 && v <= 0xFFFF);
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

	public static final TypeAdapter<Character> CHARACTER_JSON = new TypeAdapter<Character>() {
		@Override
		public void write(JsonWriter out, Character value) throws IOException {
			out.value(value);
		}

		@Override
		public Character read(JsonReader in) throws IOException {
			String v = in.nextString();
			checkArgument(v.length() == 1);
			return v.charAt(0);
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

	private static final Map<Class<?>, TypeAdapter<?>> typeAdapters = new HashMap<>();

	private static Map<Class<?>, Class<?>> PRIMITIVES = new HashMap<Class<?>, Class<?>>() {{
		put(byte.class, Byte.class);
		put(short.class, Short.class);
		put(int.class, Integer.class);
		put(long.class, Long.class);
		put(float.class, Float.class);
		put(double.class, Double.class);
		put(char.class, Character.class);
	}};

	private static Map<Class<?>, Class<?>> WRAPPERS = new HashMap<Class<?>, Class<?>>() {{
		put(Byte.class, byte.class);
		put(Short.class, short.class);
		put(Integer.class, int.class);
		put(Long.class, long.class);
		put(Float.class, float.class);
		put(Double.class, double.class);
		put(Character.class, char.class);
	}};

	private static <T> void registerPrimitive(Class<T> type, TypeAdapter<T> typeAdapter) {
		typeAdapters.put(PRIMITIVES.getOrDefault(type, type), typeAdapter);
		typeAdapters.put(WRAPPERS.getOrDefault(type, type), typeAdapter);
	}

	@SuppressWarnings("unchecked")
	public static <T> TypeAdapter<T> ofPrimitive(Class<T> type) {
		checkArgument(typeAdapters.containsKey(type));
		return (TypeAdapter<T>) typeAdapters.get(type);
	}

	public static final TypeAdapterRegistry REGISTRY = new TypeAdapterRegistryImpl();

	public interface TypeAdapterRegistry {
		TypeAdapter<?> getAdapter(Type type);
	}

	public static final class TypeAdapterRegistryImpl implements TypeAdapterRegistry {
		private TypeAdapterRegistry fallback;
		private final Map<Class<?>, TypeAdapter<?>> registeredClasses = new HashMap<>();
		private final Map<Type, TypeAdapter<?>> registeredTypes = new HashMap<>();

		private TypeAdapterRegistryImpl() {
		}

		public static TypeAdapterRegistryImpl create() {
			return new TypeAdapterRegistryImpl();
		}

		public <T> TypeAdapterRegistryImpl withFallback(TypeAdapterRegistry fallback) {
			this.fallback = fallback;
			return this;
		}

		public <T> TypeAdapterRegistryImpl with(Class<T> type, TypeAdapter<T> adapter) {
			register(type, adapter);
			return this;
		}

		public TypeAdapterRegistryImpl with(Type type, TypeAdapter<?> adapter) {
			register(type, adapter);
			return this;
		}

		public <T> TypeAdapterRegistryImpl with(TypeToken<T> type, TypeAdapter<T> adapter) {
			register(type, adapter);
			return this;
		}

		public <T> void register(Class<T> type, TypeAdapter<T> adapter) {
			registeredClasses.put(type, adapter);
		}

		public void register(Type type, TypeAdapter<?> adapter) {
			registeredTypes.put(type, adapter);
		}

		public <T> void register(TypeToken<T> type, TypeAdapter<T> adapter) {
			registeredTypes.put(type.getType(), adapter);
		}

		@Override
		public TypeAdapter<?> getAdapter(Type type) {
			checkNotNull(type);
			if (registeredTypes.containsKey(type))
				return registeredTypes.get(type);
			if (registeredClasses.containsKey(type))
				return registeredClasses.get(type);
			if (type instanceof Class<?>) {
				Class<?> clazz = (Class<?>) type;
				if (clazz.isEnum()) {
					return (TypeAdapter) ofEnum((Class) clazz);
				}
				return ofPrimitive(clazz);
			}
			if (type instanceof ParameterizedType) {
				ParameterizedType parameterizedType = (ParameterizedType) type;
				checkArgument(parameterizedType.getRawType() == Class.class);
				Class<?> rawType = (Class) parameterizedType.getRawType();
				if (List.class.isAssignableFrom(rawType)) {
					checkArgument(parameterizedType.getActualTypeArguments().length == 1, "Unsupported list type " + type);
					return ofList(getAdapter(parameterizedType.getActualTypeArguments()[0]));
				}
				if (Map.class.isAssignableFrom(rawType)) {
					checkArgument(parameterizedType.getActualTypeArguments().length == 2, "Unsupported map type " + type);
					checkArgument(parameterizedType.getActualTypeArguments()[0] == String.class);
					return ofMap(getAdapter(parameterizedType.getActualTypeArguments()[1]));
				}
			}
			if (fallback == null) {
				throw new IllegalArgumentException("Unsupported type " + type);
			}
			return fallback.getAdapter(type);
		}
	}

	static {
		registerPrimitive(Byte.class, BYTE_JSON);
		registerPrimitive(Short.class, SHORT_JSON);
		registerPrimitive(Integer.class, INTEGER_JSON);
		registerPrimitive(Long.class, LONG_JSON);
		registerPrimitive(Boolean.class, BOOLEAN_JSON);
		registerPrimitive(Float.class, FLOAT_JSON);
		registerPrimitive(Double.class, DOUBLE_JSON);
		registerPrimitive(Character.class, CHARACTER_JSON);
		typeAdapters.put(String.class, STRING_JSON);
	}

	public static <E extends Enum<E>> TypeAdapter<E> ofEnum(final Class<E> enumType) {
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

	public static <I, O> TypeAdapter<O> transform(final TypeAdapter<I> adapter, final Function<I, O> from, final Function<O, I> to) {
		return new TypeAdapter<O>() {
			@Override
			public void write(JsonWriter out, O value) throws IOException {
				adapter.write(out, to.apply(value));
			}

			@Override
			public O read(JsonReader in) throws IOException {
				return from.apply(adapter.read(in));
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
		return transform(ofList(elementAdapter),
				new Function<List<T>, T[]>() {
					@SuppressWarnings("unchecked")
					@Override
					public T[] apply(List<T> value) {
						return (T[]) value.toArray();
					}
				},
				new Function<T[], List<T>>() {
					@Override
					public List<T> apply(T[] value) {
						return Arrays.asList(value);
					}
				});
	}

	public static <T> TypeAdapter<T> asNullable(final TypeAdapter<T> adapter) {
		return new TypeAdapter<T>() {
			@Override
			public void write(JsonWriter out, T value) throws IOException {
				if (value == null) {
					out.nullValue();
				} else {
					adapter.write(out, value);
				}
			}

			@Override
			public T read(JsonReader in) throws IOException {
				if (in.peek() == JsonToken.NULL) {
					in.nextNull();
					return null;
				}
				return adapter.read(in);
			}
		};
	}

	public static <V> TypeAdapter<Map<String, V>> ofMap(final TypeAdapter<V> valueAdapter) {
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

	public static TypeAdapter<Map<String, ?>> ofHeterogeneousMap(final Map<String, ? extends TypeAdapter<?>> valueAdapters) {
		return new TypeAdapter<Map<String, ?>>() {
			@SuppressWarnings("unchecked")
			@Override
			public void write(JsonWriter out, Map<String, ?> map) throws IOException {
				out.beginObject();
				for (Map.Entry<String, ?> entry : map.entrySet()) {
					String key = entry.getKey();
					TypeAdapter valueAdapter = valueAdapters.get(key);
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
					TypeAdapter valueAdapter = valueAdapters.get(key);
					Object value = valueAdapter.read(in);
					map.put(key, value);
				}
				in.endObject();
				return map;
			}
		};
	}

	public static TypeAdapter<Object[]> ofHeterogeneousArray(final TypeAdapter<?>[] valueAdapters) {
		return new TypeAdapter<Object[]>() {
			@SuppressWarnings("unchecked")
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

	public static <T> TypeAdapter<T> oneline(final TypeAdapter<T> adapter) {
		return indent(adapter, "");
	}

	public static <T> TypeAdapter<T> indent(final TypeAdapter<T> adapter, final String indent) {
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

	public static TypeAdapter<Object[]> ofHeterogeneousArray(final List<? extends TypeAdapter<?>> valueAdapters) {
		return ofHeterogeneousArray(valueAdapters.toArray(new TypeAdapter<?>[valueAdapters.size()]));
	}

	public static TypeAdapter<List<?>> ofHeterogeneousList(final TypeAdapter<?>[] valueAdapters) {
		return transform(ofHeterogeneousArray(valueAdapters),
				new Function<Object[], List<?>>() {
					@Override
					public List<?> apply(Object[] value) {
						return Arrays.asList(value);
					}
				},
				new Function<List<?>, Object[]>() {
					@Override
					public Object[] apply(List<?> value) {
						return value.toArray();
					}
				});
	}

	public static TypeAdapter<List<?>> ofHeterogeneousList(final List<? extends TypeAdapter<?>> valueAdapters) {
		return ofHeterogeneousList(valueAdapters.toArray(new TypeAdapter<?>[valueAdapters.size()]));
	}

	public static <T> T fromJson(TypeAdapter<T> typeAdapter, String string) throws JsonException {
		StringReader reader = new StringReader(string);
		JsonReader jsonReader = new JsonReader(reader);
		return fromJson(typeAdapter, jsonReader);
	}

	@SuppressWarnings("TryWithIdenticalCatches")
	public static <T> T fromJson(TypeAdapter<T> typeAdapter, JsonReader reader) throws JsonException {
		try {
			reader.peek();
			return typeAdapter.read(reader);
		} catch (Exception e) {
			throw new JsonException(e);
		}
	}

	private static final class JsonWriterEx extends JsonWriter {
		final Writer out;

		public JsonWriterEx(Writer out) {
			super(out);
			this.out = out;
		}

		private String indentEx;

		public final void setIndentEx(String indent) {
			this.indentEx = indent;
			super.setIndent(indent);
		}

		@Override
		public JsonWriter name(String name) throws IOException {
			return super.name(name);
		}

		public final String getIndentEx() {
			return indentEx;
		}
	}

	public static <T> String toJson(TypeAdapter<T> typeAdapter, T value) throws JsonException {
		StringWriter writer = new StringWriter();
		JsonWriterEx jsonWriter = new JsonWriterEx(writer);
		jsonWriter.setLenient(true);
		jsonWriter.setIndentEx("");
		jsonWriter.setHtmlSafe(false);
		jsonWriter.setSerializeNulls(false);
		toJson(typeAdapter, jsonWriter, value);
		return writer.toString();
	}

	public static <T> void toJson(TypeAdapter<T> typeAdapter, JsonWriter writer, T value) throws JsonException {
		try {
			typeAdapter.write(writer, value);
		} catch (Exception e) {
			throw new JsonException(e);
		}
	}

}
