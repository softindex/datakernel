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
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.util.Initializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public final class TypeAdapterObjectArray<T> extends TypeAdapter<T> implements Initializable<TypeAdapterObjectArray<T>> {

	private static class Field<T, F> {
		private final TypeAdapter<F> typeAdapter;
		private final Function<T, F>  getter;
		private final BiConsumer<T, F> setter;

		private Field(TypeAdapter<F> typeAdapter, Function<T, F> getter, BiConsumer<T, F> setter) {
			this.typeAdapter = typeAdapter;
			this.getter = getter;
			this.setter = setter;
		}
	}

	private final Supplier<T> constructor;
	private final List<Field<T, ?>> fields = new ArrayList<>();

	private TypeAdapterObjectArray(Supplier<T> constructor) {
		this.constructor = constructor;
	}

	public static <T> TypeAdapterObjectArray<T> create(Supplier<T> constructor) {
		return new TypeAdapterObjectArray<>(constructor);
	}

	public <F> TypeAdapterObjectArray<T> with(TypeAdapter<F> adapter, Function<T, F> getter, BiConsumer<T, F> setter) {
		fields.add(new Field<>(adapter, getter, setter));
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(JsonWriter out, T object) throws IOException {
		out.beginArray();
		for (Field field : fields) {
			Object fieldValue = field.getter.apply(object);
			field.typeAdapter.write(out, fieldValue);
		}
		out.endArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public T read(JsonReader in) throws IOException {
		T result = constructor.get();
		in.beginArray();
		for (Field field : fields) {
			Object fieldValue = field.typeAdapter.read(in);
			field.setter.accept(result, fieldValue);
		}
		in.endArray();
		return result;
	}
}
