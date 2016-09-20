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

package io.datakernel.serializer;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.gson.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public final class GsonSubclassesAdapter<T> implements JsonSerializer<T>, JsonDeserializer<T> {
	private final BiMap<String, Class<? extends T>> classTags;
	private final Map<String, InstanceCreator<T>> classCreators;
	private final BiMap<String, Class<? extends T>> subclassNames;
	private final String subclassField;

	// region builders
	private GsonSubclassesAdapter(BiMap<String, Class<? extends T>> classTags,
	                              Map<String, InstanceCreator<T>> classCreators,
	                              String subclassField, BiMap<String, Class<? extends T>> subclassNames) {
		this.classTags = classTags;
		this.classCreators = classCreators;
		this.subclassField = subclassField;
		this.subclassNames = subclassNames;
	}

	public static <T> GsonSubclassesAdapter<T> create() {
		return new GsonSubclassesAdapter<>(
				HashBiMap.<String, Class<? extends T>>create(),
				new HashMap<String, InstanceCreator<T>>(),
				"_type",
				HashBiMap.<String, Class<? extends T>>create()
		);
	}

	public GsonSubclassesAdapter<T> withClassTag(String classTag, Class<? extends T> type,
	                                             InstanceCreator<T> instanceCreator) {
		this.classTags.put(classTag, type);
		this.classCreators.put(classTag, instanceCreator);
		return this;
	}

	public GsonSubclassesAdapter<T> withClassTag(String classTag, Class<? extends T> type) {
		this.classTags.put(classTag, type);
		return this;
	}

	public GsonSubclassesAdapter<T> withSubclassField(String subclassField) {
		return new GsonSubclassesAdapter<>(classTags, classCreators, subclassField, subclassNames);
	}

	public GsonSubclassesAdapter<T> withSubclass(String subclassName, Class<? extends T> subclass) {
		this.subclassNames.put(subclassName, subclass);
		return this;
	}
	// endregion

	private static Object newInstance(Class<?> type) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
		boolean isStatic = (type.getModifiers() & Modifier.STATIC) != 0;
		Class<?> enclosingClass = type.getEnclosingClass();
		if (isStatic || enclosingClass == null) {
			Constructor<?> ctor = type.getDeclaredConstructor();
			ctor.setAccessible(true);
			return ctor.newInstance();
		}
		Object enclosingInstance = newInstance(enclosingClass);
		Constructor<?> ctor = type.getDeclaredConstructor(enclosingClass);
		ctor.setAccessible(true);
		return ctor.newInstance(enclosingInstance);
	}

	@SuppressWarnings("unchecked")
	@Override
	public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context) {
		Class<? extends T> aClass = (Class<? extends T>) src.getClass();
		String classTag = classTags.inverse().get(aClass);
		if (classTag != null) {
			return new JsonPrimitive(classTag);
		}
		String subclassName = subclassNames.inverse().get(aClass);
		if (subclassName != null) {
			JsonObject result = new JsonObject();
			result.addProperty(subclassField, subclassName);
			JsonObject element = (JsonObject) context.serialize(src, src.getClass());
			for (Map.Entry<String, JsonElement> entry : element.entrySet()) {
				result.add(entry.getKey(), entry.getValue());
			}
			return result;
		}
		return new JsonPrimitive(src.getClass().getName());
	}

	@SuppressWarnings("unchecked")
	@Override
	public T deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
		if (json.isJsonPrimitive()) {
			if (!((JsonPrimitive) json).isString()) {
				throw new JsonParseException("Inner class name is expected");
			}
			String className = json.getAsString();
			InstanceCreator<T> creator = classCreators.get(className);
			if (creator != null) {
				return creator.createInstance(typeOfT);
			}
			try {
				Class<?> aClass = classTags.get(className);
				if (aClass == null) {
					aClass = Class.forName(className);
				}
				return (T) newInstance(aClass);
			} catch (InvocationTargetException | ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
				throw new JsonParseException(e);
			}
		}
		JsonObject object = json.getAsJsonObject();
		String subclassName = object.get(subclassField).getAsString();
		return context.deserialize(json, subclassNames.get(subclassName));
	}
}
