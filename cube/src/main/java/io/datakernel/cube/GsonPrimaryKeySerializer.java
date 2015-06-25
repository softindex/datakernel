/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.cube;

import com.google.gson.*;
import com.google.gson.internal.Primitives;

import java.lang.reflect.Type;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class GsonPrimaryKeySerializer implements JsonSerializer<PrimaryKey>, JsonDeserializer<PrimaryKey> {
	private final Class<?>[] types;

	public GsonPrimaryKeySerializer(Class<?>[] types) {
		this.types = types;
	}

	public static GsonPrimaryKeySerializer ofDimensions(Map<String, Class<?>> dimensions) {
		Class<?>[] types = new Class[dimensions.size()];
		int i = 0;
		for (Class<?> rawType : dimensions.values()) {
			types[i++] = Primitives.wrap(rawType);
		}
		return new GsonPrimaryKeySerializer(types);
	}

	@Override
	public JsonElement serialize(PrimaryKey src, Type typeOfSrc, JsonSerializationContext context) {
		checkArgument(src.values().size() == types.length);
		JsonArray jsonArray = new JsonArray();
		for (int i = 0; i < types.length; i++) {
			Object value = src.values().get(i);
			Class<?> type = types[i];
			checkNotNull(value);
			checkNotNull(type);
			JsonElement element = context.serialize(value, type);
			jsonArray.add(element);
		}
		return jsonArray;
	}

	@Override
	public PrimaryKey deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
		JsonArray array = (JsonArray) json;
		Object[] values = new Object[types.length];
		for (int i = 0; i < types.length; i++) {
			JsonElement jsonElement = array.get(i);
			Class<?> type = types[i];
			checkNotNull(jsonElement);
			checkNotNull(type);
			Object value = context.deserialize(jsonElement, type);
			values[i] = value;
		}
		return PrimaryKey.ofArray(values);
	}

	public static Gson gson(Map<String, Class<?>> dimensions) {
		return new GsonBuilder()
				.registerTypeAdapter(PrimaryKey.class, ofDimensions(dimensions))
				.create();
	}

	public static String serializePrimaryKey(Map<String, Class<?>> dimensions, PrimaryKey primaryKey) {
		return gson(dimensions).toJson(primaryKey, PrimaryKey.class);
	}

	public static PrimaryKey deserializePrimaryKey(Map<String, Class<?>> dimensions, String primaryKey) {
		return gson(dimensions).fromJson(primaryKey, PrimaryKey.class);
	}
}
