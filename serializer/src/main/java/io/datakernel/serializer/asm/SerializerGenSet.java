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

import io.datakernel.bytebuf.SerializationUtils;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.util.Preconditions;

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static io.datakernel.codegen.Expressions.*;

public class SerializerGenSet implements SerializerGen, NullableOptimization {
	private final SerializerGen valueSerializer;
	private final boolean nullable;
	private final Class<?> elementType;

	public SerializerGenSet(SerializerGen valueSerializer, Class<?> type) {
		this.valueSerializer = valueSerializer;
		this.nullable = false;
		this.elementType = type;
	}

	public SerializerGenSet(SerializerGen valueSerializer, boolean nullable, Class<?> type) {
		this.valueSerializer = valueSerializer;
		this.nullable = nullable;
		this.elementType = type;
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
		return Set.class;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		Expression serializeEach = forEach(value, valueSerializer.getRawType(),
				it -> set(off, valueSerializer.serialize(arg(0), arg(1), it, version, staticMethods, compatibilityLevel)));

		if (nullable) {
			return ifThenElse(isNull(value),
					sequence(set(off, callStatic(SerializationUtils.class, "writeVarInt", byteArray, off, value(0))), off),
					sequence(set(off, callStatic(SerializationUtils.class, "writeVarInt", byteArray, off, inc(call(value, "size")))),
							serializeEach, off));
		}
		return sequence(
				set(off, callStatic(SerializationUtils.class, "writeVarInt", byteArray, off, call(value, "size"))),
				serializeEach, off);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		boolean isEnum = valueSerializer.getRawType().isEnum();
		Class<?> targetInstance = isEnum ? EnumSet.class : LinkedHashSet.class;
		Preconditions.check(targetType.isAssignableFrom(targetInstance));

		return deserializeSet(version, staticMethods, compatibilityLevel, isEnum);
	}

	private Expression deserializeSet(int version,
			SerializerBuilder.StaticMethods staticMethods,
			CompatibilityLevel compatibilityLevel, boolean isEnum) {
		Expression length = let(call(arg(0), "readVarInt"));
		Expression container = isEnum ?
				let(callStatic(EnumSet.class, "noneOf", value(elementType))) :
				let(constructor(LinkedHashSet.class, (!nullable ? length : dec(length))));
		Expression deserializeEach = expressionFor(value(0), !nullable ? length : dec(length),
				it -> sequence(
						call(container, "add", cast(valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods, compatibilityLevel), Object.class)),
						voidExp()));
		if (!nullable) {
			return sequence(length, container, deserializeEach, container);
		}
		return ifThenElse(cmpEq(length, value(0)),
				nullRef(Set.class),
				sequence(container, deserializeEach, container));
	}

	@Override
	public SerializerGen setNullable() {
		return new SerializerGenSet(valueSerializer, true, elementType);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenSet that = (SerializerGenSet) o;

		if (nullable != that.nullable) return false;
		return !(valueSerializer != null ? !valueSerializer.equals(that.valueSerializer) : that.valueSerializer != null);

	}

	@Override
	public int hashCode() {
		int result = valueSerializer != null ? valueSerializer.hashCode() : 0;
		result = 31 * result + (nullable ? 1 : 0);
		return result;
	}
}
