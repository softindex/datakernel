package io.datakernel.utils;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public final class TypeAdapterObject<T> extends TypeAdapter<T> {
	public interface Constructor<T> {
		T construct();
	}

	public interface Getter<T, F> {
		F get(T object);
	}

	public interface Setter<T, F> {
		void set(T object, F value);
	}

	private static class Field<T, F> {
		private final TypeAdapter<F> typeAdapter;
		private final Getter<T, F> getter;
		private final Setter<T, F> setter;

		private Field(TypeAdapter<F> typeAdapter, Getter<T, F> getter, Setter<T, F> setter) {
			this.typeAdapter = typeAdapter;
			this.getter = getter;
			this.setter = setter;
		}
	}

	private final Constructor<T> constructor;
	private final Map<String, Field<T, ?>> fields = new LinkedHashMap<>();

	private TypeAdapterObject(Constructor<T> constructor) {
		this.constructor = constructor;
	}

	public static <T> TypeAdapterObject<T> create(Constructor<T> constructor) {
		return new TypeAdapterObject<>(constructor);
	}

	public <F> TypeAdapterObject<T> with(String field, TypeAdapter<F> adapter,
	                                     Getter<T, F> getter, Setter<T, F> setter) {
		fields.put(field, new Field<>(adapter, getter, setter));
		return this;
	}

	@Override
	public void write(JsonWriter out, T object) throws IOException {
		out.beginObject();
		for (Map.Entry<String, Field<T, ?>> entry : fields.entrySet()) {
			out.name(entry.getKey());
			Field<T, Object> field = (Field<T, Object>) entry.getValue();
			Object fieldValue = field.getter.get(object);
			field.typeAdapter.write(out, fieldValue);
		}
		out.endObject();
	}

	@Override
	public T read(JsonReader in) throws IOException {
		T result = constructor.construct();
		in.beginObject();
		while (in.hasNext()) {
			String name = in.nextName();
			Field<T, Object> field = (Field<T, Object>) fields.get(name);
			Object fieldValue = field.typeAdapter.read(in);
			field.setter.set(result, fieldValue);
		}
		in.endObject();
		return result;
	}

}
