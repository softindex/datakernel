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

package io.datakernel.serializer.asm;

import io.datakernel.codegen.ForVar;
import io.datakernel.codegen.FunctionDef;
import io.datakernel.serializer.SerializerFactory;

import java.util.HashMap;
import java.util.Map;

import static io.datakernel.codegen.FunctionDefs.*;
import static io.datakernel.codegen.utils.Preconditions.check;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;

public class SerializerGenHppcSet implements SerializerGen {

	private static Map<Class<?>, SerializerGen> primitiveSerializers = new HashMap<Class<?>, SerializerGen>() {{
		put(byte.class, new SerializerGenByte());
		put(short.class, new SerializerGenShort());
		put(int.class, new SerializerGenInt(true));
		put(long.class, new SerializerGenLong(false));
		put(float.class, new SerializerGenFloat());
		put(double.class, new SerializerGenDouble());
		put(char.class, new SerializerGenChar());
	}};

	private static String toUpperCamel(String s) {
		StringBuilder sb = new StringBuilder();
		sb.append(Character.toUpperCase(s.charAt(0)));
		sb.append(s.substring(1));
		return sb.toString();
	}

	public static SerializerGenBuilder serializerGenBuilder(final Class<?> setType, final Class<?> valueType) {
		String prefix = toUpperCamel(valueType.getSimpleName());
		check(setType.getSimpleName().startsWith(prefix), "Expected setType '%s', but was begin '%s'", setType.getSimpleName(), prefix);
		return new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, final SerializerForType[] generics, SerializerGen fallback) {
				SerializerGen valueSerializer;
				if (generics.length == 1) {
					check(valueType == Object.class, "valueType must be Object.class");
					valueSerializer = generics[0].serializer;
				} else {
					valueSerializer = primitiveSerializers.get(valueType);
				}
				return new SerializerGenHppcSet(setType, valueType, checkNotNull(valueSerializer));
			}
		};
	}

	private final Class<?> setType;
	private final Class<?> hashSetType;
	private final Class<?> iteratorType;
	private final Class<?> valueType;
	private final SerializerGen valueSerializer;

	public SerializerGenHppcSet(Class<?> setType, Class<?> valueType, SerializerGen valueSerializer) {
		this.setType = setType;
		this.valueType = valueType;
		this.valueSerializer = valueSerializer;
		try {
			String prefix = toUpperCamel(valueType.getSimpleName());
			this.iteratorType = Class.forName("com.carrotsearch.hppc.cursors." + prefix + "Cursor");
			this.hashSetType = Class.forName("com.carrotsearch.hppc." + prefix + "OpenHashSet");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void getVersions(VersionsCollector versions) {
		versions.addRecursive(valueSerializer);
	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return setType;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerFactory.StaticMethods staticMethods) {
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods);
	}

	@Override
	public FunctionDef serialize(FunctionDef value, final int version, final SerializerFactory.StaticMethods staticMethods) {
		FunctionDef length = call(arg(0), "writeVarInt", call(value, "size"));
		return sequence(length, hppcSetForEach(iteratorType, value, new ForVar() {
			@Override
			public FunctionDef forVar(FunctionDef local) {
				return valueSerializer.serialize(local, version, staticMethods);
			}
		}));
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerFactory.StaticMethods staticMethods) {
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods);
	}

	@Override
	public FunctionDef deserialize(Class<?> targetType, final int version, final SerializerFactory.StaticMethods staticMethods) {
		final Class<?> valueType = valueSerializer.getRawType();
		FunctionDef length = let(call(arg(0), "readVarInt"));
		final FunctionDef set = let(constructor(hashSetType));
		return sequence(set, functionFor(length, new ForVar() {
			@Override
			public FunctionDef forVar(FunctionDef local) {
				return sequence(
						call(set, "add", cast(valueSerializer.deserialize(valueType, version, staticMethods), SerializerGenHppcSet.this.valueType)),
						voidFunc()
				);
			}
		}), set);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		SerializerGenHppcSet that = (SerializerGenHppcSet) o;

		if (!valueSerializer.equals(that.valueSerializer))
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = 31 * valueSerializer.hashCode();
		return result;
	}
}
