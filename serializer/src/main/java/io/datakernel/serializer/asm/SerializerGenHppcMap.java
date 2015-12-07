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

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.ForVar;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializationOutputBuffer;
import io.datakernel.serializer.SerializerBuilder;

import java.util.HashMap;
import java.util.Map;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.utils.Preconditions.check;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;

@SuppressWarnings("PointlessArithmeticExpression")
public final class SerializerGenHppcMap implements SerializerGen {

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

	public static SerializerGenBuilder serializerGenBuilder(final Class<?> mapType, final Class<?> keyType, final Class<?> valueType) {
		String prefix = toUpperCamel(keyType.getSimpleName()) + toUpperCamel(valueType.getSimpleName());
		check(mapType.getSimpleName().startsWith(prefix), "Expected mapType '%s', but was begin '%s'", mapType.getSimpleName(), prefix);
		return new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, final SerializerForType[] generics, SerializerGen fallback) {
				SerializerGen keySerializer;
				SerializerGen valueSerializer;
				if (generics.length == 2) {
					check((keyType == Object.class) && (valueType == Object.class), "keyType and valueType must be Object.class");
					keySerializer = generics[0].serializer;
					valueSerializer = generics[1].serializer;
				} else if (generics.length == 1) {
					check((keyType == Object.class) || (valueType == Object.class), "keyType or valueType must be Object.class");
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
				return new SerializerGenHppcMap(mapType, keyType, valueType, checkNotNull(keySerializer), checkNotNull(valueSerializer));
			}
		};
	}

	private final Class<?> mapType;
	private final Class<?> hashMapType;
	private final Class<?> iteratorType;
	private final Class<?> keyType;
	private final Class<?> valueType;
	private final SerializerGen keySerializer;
	private final SerializerGen valueSerializer;

	private SerializerGenHppcMap(Class<?> mapType, Class<?> keyType, Class<?> valueType, SerializerGen keySerializer, SerializerGen valueSerializer) {
		this.mapType = mapType;
		this.keyType = keyType;
		this.valueType = valueType;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		try {
			String prefix = toUpperCamel(keyType.getSimpleName()) + toUpperCamel(valueType.getSimpleName());
			this.iteratorType = Class.forName("com.carrotsearch.hppc.cursors." + prefix + "Cursor");
			this.hashMapType = Class.forName("com.carrotsearch.hppc." + prefix + "OpenHashMap");
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
	public Class<?> getRawType() throws RuntimeException {
		return mapType;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		keySerializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression serialize(final Expression byteArray, final Variable off, Expression value, final int version, final SerializerBuilder.StaticMethods staticMethods, final CompatibilityLevel compatibilityLevel) {
		Expression length = set(off, callStatic(SerializationOutputBuffer.class, "writeVarInt", byteArray, off, call(value, "size")));
		return sequence(length, hppcMapForEach(iteratorType, value,
				new ForVar() {
					@Override
					public Expression forVar(Expression it) {
						return set(off, keySerializer.serialize(byteArray, off, cast(it, keySerializer.getRawType()), version, staticMethods, compatibilityLevel));
					}
				},
				new ForVar() {
					@Override
					public Expression forVar(Expression it) {
						return set(off, valueSerializer.serialize(byteArray, off, cast(it, valueSerializer.getRawType()), version, staticMethods, compatibilityLevel));
					}
				}), off);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		keySerializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression deserialize(Class<?> targetType, final int version, final SerializerBuilder.StaticMethods staticMethods, final CompatibilityLevel compatibilityLevel) {
		Expression length = let(call(arg(0), "readVarInt"));
		final Expression map = let(constructor(hashMapType));
		final Class<?> valueType = valueSerializer.getRawType();
		final Class<?> keyType = keySerializer.getRawType();
		return sequence(length, map, expressionFor(length, new ForVar() {
			@Override
			public Expression forVar(Expression it) {
				return sequence(call(map, "put",
								cast(keySerializer.deserialize(keyType, version, staticMethods, compatibilityLevel), SerializerGenHppcMap.this.keyType),
								cast(valueSerializer.deserialize(valueType, version, staticMethods, compatibilityLevel), SerializerGenHppcMap.this.valueType)
						), voidExp()
				);
			}
		}), map);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		SerializerGenHppcMap that = (SerializerGenHppcMap) o;

		if (!keySerializer.equals(that.keySerializer))
			return false;
		if (!valueSerializer.equals(that.valueSerializer))
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = keySerializer.hashCode();
		result = 31 * result + valueSerializer.hashCode();
		return result;
	}
}
