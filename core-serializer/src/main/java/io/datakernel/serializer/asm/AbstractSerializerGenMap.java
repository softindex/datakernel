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

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.util.BinaryOutputUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.function.Function;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.common.Preconditions.checkArgument;
import static java.util.Collections.emptySet;

public abstract class AbstractSerializerGenMap implements SerializerGen, NullableOptimization {
	protected final SerializerGen keySerializer;
	protected final SerializerGen valueSerializer;
	protected final Class<?> mapType;
	protected final Class<?> mapImplType;
	protected final Class<?> keyType;
	protected final Class<?> valueType;
	protected final boolean nullable;

	protected AbstractSerializerGenMap(@NotNull SerializerGen keySerializer, @NotNull SerializerGen valueSerializer, @NotNull Class<?> mapType, @NotNull Class<?> mapImplType, @NotNull Class<?> keyType, @NotNull Class<?> valueType, boolean nullable) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.mapType = mapType;
		this.mapImplType = mapImplType;
		this.keyType = keyType;
		this.valueType = valueType;
		this.nullable = nullable;
	}

	protected abstract Expression mapForEach(Expression collection, Function<Expression, Expression> forEachKey, Function<Expression, Expression> forEachValue);

	protected Expression createConstructor(Expression length) {
		return constructor(mapImplType, !nullable ? length : dec(length));
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visit("key", keySerializer);
		visitor.visit("value", valueSerializer);
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
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
	public final Expression serialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		Expression length = length(value);
		Expression writeLength = set(off, callStatic(BinaryOutputUtils.class, "writeVarInt", byteArray, off, (!nullable ? length : inc(length))));
		Expression forEach = mapForEach(value,
				k -> set(off, keySerializer.serialize(classLoader, byteArray, off, cast(k, keySerializer.getRawType()), version, compatibilityLevel)),
				v -> set(off, valueSerializer.serialize(classLoader, byteArray, off, cast(v, valueSerializer.getRawType()), version, compatibilityLevel)));

		if (!nullable) {
			return sequence(writeLength, forEach, off);
		} else {
			return ifThenElse(isNull(value),
					sequence(set(off, callStatic(BinaryOutputUtils.class, "writeVarInt", byteArray, off, value(0))), off),
					sequence(writeLength, forEach, off));
		}
	}

	@Override
	public final Expression deserialize(DefiningClassLoader classLoader, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		checkArgument(targetType.isAssignableFrom(mapImplType), "Target(%s) should be assignable from map implementation type(%s)", targetType, mapImplType);
		return let(call(arg(0), "readVarInt"), length ->
				!nullable ?
						let(createConstructor(length), instance -> sequence(
								loop(value(0), length,
										it -> sequence(
												call(instance, "put",
														cast(keySerializer.deserialize(classLoader, keySerializer.getRawType(), version, compatibilityLevel), keyType),
														cast(valueSerializer.deserialize(classLoader, valueSerializer.getRawType(), version, compatibilityLevel), valueType)
												),
												voidExp())),
								instance)) :
						ifThenElse(
								cmpEq(length, value(0)),
								nullRef(mapImplType),
								let(createConstructor(length), instance -> sequence(
										loop(value(0), dec(length),
												it -> sequence(
														call(instance, "put",
																cast(keySerializer.deserialize(classLoader, keySerializer.getRawType(), version, compatibilityLevel), keyType),
																cast(valueSerializer.deserialize(classLoader, valueSerializer.getRawType(), version, compatibilityLevel), valueType)
														),
														voidExp())),
										instance))));
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
		if (!mapImplType.equals(that.mapImplType)) return false;
		return true;
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
