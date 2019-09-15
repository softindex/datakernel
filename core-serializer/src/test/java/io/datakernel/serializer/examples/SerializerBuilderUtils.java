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

package io.datakernel.serializer.examples;

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.asm.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.common.Preconditions.*;
import static io.datakernel.common.Preconditions.checkArgument;
import static java.lang.Character.toUpperCase;
import static java.util.Arrays.asList;

public class SerializerBuilderUtils {
	public static final List<Class<?>> TYPES = asList(
			byte.class, short.class, int.class, long.class, float.class, double.class, char.class, Object.class
	);
	private static Map<Class<?>, SerializerGen> primitiveSerializers = new HashMap<Class<?>, SerializerGen>() {{
		put(byte.class, new SerializerGenByte());
		put(short.class, new SerializerGenShort());
		put(int.class, new SerializerGenInt(true));
		put(long.class, new SerializerGenLong(false));
		put(float.class, new SerializerGenFloat());
		put(double.class, new SerializerGenDouble());
		put(char.class, new SerializerGenChar());
	}};

	private static Map<String, String> collectionImplSuffix = new HashMap<String, String>() {{
		put("Set", "HashSet");
		put("IndexedContainer", "ArrayList");
	}};

	// region creators
	public static SerializerBuilder createWithHppc7Support(String profile, DefiningClassLoader definingClassLoader) {
		SerializerBuilder builder = SerializerBuilder.create(profile, definingClassLoader);
		return register(builder, definingClassLoader);
	}

	public static SerializerBuilder createWithHppc7Support(DefiningClassLoader definingClassLoader) {
		SerializerBuilder builder = SerializerBuilder.create(definingClassLoader);
		return register(builder, definingClassLoader);

	}

	// endregion
	private static SerializerBuilder register(SerializerBuilder builder, DefiningClassLoader definingClassLoader) {
		registerHppcMaps(builder, definingClassLoader);
		registerHppcCollections(builder, definingClassLoader);
		return builder;
	}

	private static void registerHppcMaps(SerializerBuilder builder, DefiningClassLoader classLoader) {
		for (int i = 0; i < TYPES.size(); i++) {
			Class<?> keyType = TYPES.get(i);
			String keyTypeName = keyType.getSimpleName();
			for (Class<?> valueType : TYPES) {
				String valueTypeName = valueType.getSimpleName();
				String prefix = "com.carrotsearch.hppc." + capitalize(keyTypeName) + capitalize(valueTypeName);
				String hppcMapTypeName = prefix + "Map";
				String hppcMapImplTypeName = prefix + "HashMap";
				Class<?> hppcMapType, hppcMapImplType;
				try {
					hppcMapType = Class.forName(hppcMapTypeName, true, classLoader);
					hppcMapImplType = Class.forName(hppcMapImplTypeName, true, classLoader);
				} catch (ClassNotFoundException e) {
					throw new IllegalStateException("There is no collection with given name" + e.getClass().getName(), e);
				}
				builder.withSerializer(hppcMapType, serializerGenMapBuilder(hppcMapType, hppcMapImplType, keyType, valueType));
			}
		}
	}

	private static void registerHppcCollections(SerializerBuilder builder, DefiningClassLoader classLoader) {
		for (Map.Entry<String, String> collectionImpl : collectionImplSuffix.entrySet()) {
			for (Class<?> valueType : TYPES) {
				String valueTypeName = valueType.getSimpleName();
				String prefix = "com.carrotsearch.hppc." + capitalize(valueTypeName);
				String hppcCollectionTypeName = prefix + collectionImpl.getKey();
				String hppcCollectionTypeImplName = prefix + collectionImpl.getValue();
				Class<?> hppcCollectionType, hppcCollectionTypeImpl;
				try {
					hppcCollectionType = Class.forName(hppcCollectionTypeName, true, classLoader);
					hppcCollectionTypeImpl = Class.forName(hppcCollectionTypeImplName, true, classLoader);
				} catch (ClassNotFoundException e) {
					throw new IllegalStateException("There is no collection with given name", e);
				}
				builder.withSerializer(hppcCollectionType, serializerGenCollectionBuilder(hppcCollectionType, hppcCollectionTypeImpl, valueType));
			}
		}
	}

	public static String capitalize(String str) {
		return toUpperCase(str.charAt(0)) + str.substring(1);
	}

	private static SerializerGenBuilder serializerGenMapBuilder(Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType) {
		String prefix = capitalize(keyType.getSimpleName()) + capitalize(valueType.getSimpleName());
		checkArgument(mapType.getSimpleName().startsWith(prefix), "Expected mapType '%s', but was begin '%s'", mapType.getSimpleName(), prefix);
		return (type, generics, fallback) -> {
			SerializerGen keySerializer;
			SerializerGen valueSerializer;
			if (generics.length == 2) {
				checkArgument((keyType == Object.class) && (valueType == Object.class), "keyType and valueType must be Object.class");
				keySerializer = generics[0].serializer;
				valueSerializer = generics[1].serializer;
			} else if (generics.length == 1) {
				checkArgument((keyType == Object.class) || (valueType == Object.class), "keyType or valueType must be Object.class");
				if (keyType == Object.class) {
					keySerializer = generics[0].serializer;
					valueSerializer = primitiveSerializers.get(valueType);
				} else {
					keySerializer = primitiveSerializers.get(keyType);
					valueSerializer = generics[0].serializer;
				}
			} else {
				keySerializer = primitiveSerializers.get(keyType);
				valueSerializer = primitiveSerializers.get(valueType);
			}
			return new SerializerGenHppc7Map(checkNotNull(keySerializer), checkNotNull(valueSerializer), mapType, mapImplType, keyType, valueType);
		};
	}

	private static SerializerGenBuilder serializerGenCollectionBuilder(Class<?> collectionType, Class<?> collectionImplType, Class<?> valueType) {
		String prefix = capitalize(valueType.getSimpleName());
		checkArgument(collectionType.getSimpleName().startsWith(prefix), "Expected setType '%s', but was begin '%s'", collectionType.getSimpleName(), prefix);
		return (type, generics, fallback) -> {
			SerializerGen valueSerializer;
			if (generics.length == 1) {
				checkArgument(valueType == Object.class, "valueType must be Object.class");
				valueSerializer = generics[0].serializer;
			} else {
				valueSerializer = primitiveSerializers.get(valueType);
			}
			return new SerializerGenHppc7Collection(collectionType, collectionImplType, valueType, checkNotNull(valueSerializer));
		};
	}
}
