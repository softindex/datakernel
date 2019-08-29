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

package io.datakernel.serializer.asm;

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.ExpressionParameter;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.SerializerBuilder.StaticMethods;
import io.datakernel.serializer.util.BinaryOutputUtils;

import java.util.function.Function;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;

public abstract class AbstractSerializerGenMap implements SerializerGen, NullableOptimization {
	protected final SerializerGen keySerializer;
	protected final SerializerGen valueSerializer;
	protected final Class<?> mapType;
	protected final Class<?> mapImplType;
	protected final Class<?> keyType;
	protected final Class<?> valueType;
	protected final boolean nullable;

	protected AbstractSerializerGenMap(SerializerGen keySerializer, SerializerGen valueSerializer, Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType, boolean nullable) {
		this.keySerializer = checkNotNull(keySerializer);
		this.valueSerializer = checkNotNull(valueSerializer);
		this.mapType = checkNotNull(mapType);
		this.mapImplType = checkNotNull(mapImplType);
		this.keyType = checkNotNull(keyType);
		this.valueType = checkNotNull(valueType);
		this.nullable = nullable;
	}

	protected abstract Expression mapForEach(Expression collection, Function<ExpressionParameter, Expression> key, Function<ExpressionParameter, Expression> value);

	protected Expression createConstructor(Expression length) {
		return let(constructor(mapImplType, !nullable ? length : dec(length)));
	}

	@Override
	public void getVersions(VersionsCollector versions) {
		versions.addRecursive(valueSerializer);
	}

	@Override
	public Class<?> getRawType() {
		return mapType;
	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		keySerializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public final Expression serialize(Expression byteArray, Variable off, Expression value, int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		Expression length = length(value);
		Expression writeLength = set(off, callStatic(BinaryOutputUtils.class, "writeVarInt", byteArray, off, (!nullable ? length : inc(length))));
		Expression forEach = mapForEach(value,
				k -> set(off, keySerializer.serialize(byteArray, off, cast(k, keySerializer.getRawType()), version, staticMethods, compatibilityLevel)),
				v -> set(off, valueSerializer.serialize(byteArray, off, cast(v, valueSerializer.getRawType()), version, staticMethods, compatibilityLevel)));

		if (!nullable) {
			return sequence(writeLength, forEach, off);
		} else {
			return ifThenElse(isNull(value),
					sequence(set(off, callStatic(BinaryOutputUtils.class, "writeVarInt", byteArray, off, value(0))), off),
					sequence(writeLength, forEach, off));
		}
	}

	@Override
	public final Expression deserialize(Class<?> targetType, int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		check(targetType.isAssignableFrom(mapImplType), "Target(%s) should be assignable from map implementation type(%s)", targetType, mapImplType);
		return let(
				call(arg(0), "readVarInt"),
				length -> {
					Expression container = createConstructor(length);
					Expression forEach = expressionFor(value(0), (!nullable ? length : dec(length)),
							it -> sequence(
									call(container, "put",
											cast(keySerializer.deserialize(keySerializer.getRawType(), version, staticMethods, compatibilityLevel), keyType),
											cast(valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods, compatibilityLevel), valueType)
									),
									voidExp()));
					if (!nullable) {
						return sequence(container, forEach, container);
					} else {
						return ifThenElse(cmpEq(length, value(0)),
								nullRef(mapImplType),
								sequence(container, forEach, container));
					}
				});
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		keySerializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof AbstractSerializerGenMap)) return false;

		AbstractSerializerGenMap that = (AbstractSerializerGenMap) o;

		if (nullable != that.nullable) return false;
		if (!keySerializer.equals(that.keySerializer)) return false;
		if (!valueSerializer.equals(that.valueSerializer)) return false;
		if (!mapType.equals(that.mapType)) return false;
		if (!keyType.equals(that.keyType)) return false;
		if (!valueType.equals(that.valueType)) return false;
		return mapImplType.equals(that.mapImplType);
	}

	@Override
	public int hashCode() {
		int result = keySerializer.hashCode();
		result = 31 * result + valueSerializer.hashCode();
		result = 31 * result + (nullable ? 1 : 0);
		result = 31 * result + mapType.hashCode();
		result = 31 * result + keyType.hashCode();
		result = 31 * result + valueType.hashCode();
		result = 31 * result + mapImplType.hashCode();
		return result;
	}
}
