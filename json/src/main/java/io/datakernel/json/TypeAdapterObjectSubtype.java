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
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static java.lang.Math.max;

public class TypeAdapterObjectSubtype<T> extends TypeAdapter<T> implements Initializable<TypeAdapterObjectSubtype<T>> {

	private final Map<Type, TypeAdapter<? extends T>> classesToAdapters = new HashMap<>();

	private final Map<String, TypeAdapter<? extends T>> namesToAdapters = new HashMap<>();
	private final Map<Type, String> subtypesToNames = new HashMap<>();

	private final Map<Type, String> statelessSubtypesToNames = new HashMap<>();
	private final Map<String, Supplier<? extends T>> namesToStatelessSubtypes = new HashMap<>();

	private boolean allOtherAreStateless = false;

	private TypeAdapterObjectSubtype() {
	}

	public static <T> TypeAdapterObjectSubtype<T> create() {
		return new TypeAdapterObjectSubtype<>();
	}

	public TypeAdapterObjectSubtype<T> withSubtype(Type type, String name, TypeAdapter<? extends T> adapter) {
		namesToAdapters.put(name, adapter);
		subtypesToNames.put(type, name);
		classesToAdapters.put(type, adapter);
		return this;
	}

	public TypeAdapterObjectSubtype<T> withSubtype(Type type, TypeAdapter<? extends T> adapter) {
		String name = type.getTypeName();
		name = name.substring(max(name.lastIndexOf('.'), name.lastIndexOf('$')) + 1);
		return withSubtype(type, name, adapter);
	}

	public TypeAdapterObjectSubtype<T> withStatelessSubtype(Supplier<? extends T> constructor, String name) {
		statelessSubtypesToNames.put(constructor.get().getClass(), name);
		namesToStatelessSubtypes.put(name, constructor);
		return this;
	}

	public TypeAdapterObjectSubtype<T> allOtherAreStateless() {
		allOtherAreStateless = true;
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(JsonWriter out, T value) throws IOException {
		String stateless = statelessSubtypesToNames.get(value.getClass());
		if (stateless != null) {
			out.value(stateless);
			return;
		}
		Class<?> cls = value.getClass();
		String name = subtypesToNames.get(cls);
		if (name == null && allOtherAreStateless) {
			out.value(cls.getName());
			return;
		}
		out.beginObject();
		out.name(Objects.requireNonNull(name));
		((TypeAdapter<T>) classesToAdapters.get(cls)).write(out, value);
		out.endObject();
	}

	@Override
	public T read(JsonReader in) throws IOException {
		switch (in.peek()) {
			case BEGIN_OBJECT:
				in.beginObject();
				T value = namesToAdapters.get(in.nextName()).read(in);
				in.endObject();
				return value;
			case STRING:
				String key = in.nextString();
				if (allOtherAreStateless) {
					return namesToStatelessSubtypes.getOrDefault(key, () -> GsonAdapters.newInstance(key)).get();
				}
				return namesToStatelessSubtypes.get(key).get();
			default:
				throw new IOException("Wrong token here!");
		}
	}
}
