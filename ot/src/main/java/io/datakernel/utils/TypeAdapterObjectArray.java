package io.datakernel.utils;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class TypeAdapterObjectArray<T> extends TypeAdapter<T> {
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
	private final List<Field<T, ?>> fields = new ArrayList<>();

	private TypeAdapterObjectArray(Constructor<T> constructor) {
		this.constructor = constructor;
	}

	public static <T> TypeAdapterObjectArray<T> create(Constructor<T> constructor) {
		return new TypeAdapterObjectArray<>(constructor);
	}

	public <F> TypeAdapterObjectArray<T> with(TypeAdapter<F> adapter,
	                                          Getter<T, F> getter, Setter<T, F> setter) {
		fields.add(new Field<>(adapter, getter, setter));
		return this;
	}

	@Override
	public void write(JsonWriter out, T object) throws IOException {
		out.beginArray();
		for (Field field : fields) {
			Object fieldValue = field.getter.get(object);
			field.typeAdapter.write(out, fieldValue);
		}
		out.endArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public T read(JsonReader in) throws IOException {
		T result = constructor.construct();
		in.beginArray();
		for (Field field : fields) {
			Object fieldValue = field.typeAdapter.read(in);
			field.setter.set(result, fieldValue);
		}
		in.endArray();
		return result;
	}

}
