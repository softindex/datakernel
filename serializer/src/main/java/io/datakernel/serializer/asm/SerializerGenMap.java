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

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;
import static org.objectweb.asm.Type.getType;

@SuppressWarnings("PointlessArithmeticExpression")
public final class SerializerGenMap implements SerializerGen {
	private final SerializerGen keySerializer;
	private final SerializerGen valueSerializer;

	public SerializerGenMap(SerializerGen keySerializer, SerializerGen valueSerializer) {
		this.keySerializer = checkNotNull(keySerializer);
		this.valueSerializer = checkNotNull(valueSerializer);
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
		return Map.class;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		keySerializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression serialize(final Expression byteArray, final Variable off, Expression value, final int version, final SerializerBuilder.StaticMethods staticMethods, final CompatibilityLevel compatibilityLevel) {
		Expression length = set(off, callStatic(SerializationOutputBuffer.class, "writeVarInt", byteArray, off, length(value)));

		return sequence(length, mapForEach(value,
						new ForVar() {
							@Override
							public Expression forVar(Expression it) {return set(off, keySerializer.serialize(byteArray, off, cast(it, keySerializer.getRawType()), version, staticMethods, compatibilityLevel));}
						},
						new ForVar() {
							@Override
							public Expression forVar(Expression it) {return set(off, valueSerializer.serialize(byteArray, off, cast(it, valueSerializer.getRawType()), version, staticMethods, compatibilityLevel));}
						}),
				off
		);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		keySerializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression deserialize(Class<?> targetType, final int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		boolean isEnum = keySerializer.getRawType().isEnum();

		if (!isEnum) {
			return deserializeSimple(version, staticMethods, compatibilityLevel);
		} else {
			return deserializeEnum(version, staticMethods, compatibilityLevel);
		}
	}

	public Expression deserializeSimple(final int version, final SerializerBuilder.StaticMethods staticMethods, final CompatibilityLevel compatibilityLevel) {
		Expression length = let(call(arg(0), "readVarInt"));
		final Expression local = let(constructor(LinkedHashMap.class, length));
		Expression forEach = expressionFor(length, new ForVar() {
			@Override
			public Expression forVar(Expression it) {
				return sequence(call(local, "put",
						cast(keySerializer.deserialize(keySerializer.getRawType(), version, staticMethods, compatibilityLevel), Object.class),
						cast(valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods, compatibilityLevel), Object.class)
				), voidExp());
			}
		});
		return sequence(length, local, forEach, local);
	}

	public Expression deserializeEnum(final int version, final SerializerBuilder.StaticMethods staticMethods, final CompatibilityLevel compatibilityLevel) {
		Expression length = let(call(arg(0), "readVarInt"));
		final Expression localMap = let(constructor(EnumMap.class, cast(value(getType(keySerializer.getRawType())), Class.class)));
		Expression forEach = expressionFor(length, new ForVar() {
			@Override
			public Expression forVar(Expression it) {
				return sequence(call(localMap, "put",
						cast(keySerializer.deserialize(keySerializer.getRawType(), version, staticMethods, compatibilityLevel), Object.class),
						cast(valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods, compatibilityLevel), Object.class)
				), voidExp());
			}
		});
		return sequence(length, localMap, forEach, localMap);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenMap that = (SerializerGenMap) o;

		if (!keySerializer.equals(that.keySerializer)) return false;
		if (!valueSerializer.equals(that.valueSerializer)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = keySerializer.hashCode();
		result = 31 * result + valueSerializer.hashCode();
		return result;
	}
}
