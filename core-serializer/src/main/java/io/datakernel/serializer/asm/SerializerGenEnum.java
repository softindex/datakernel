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

import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.serializer.asm.SerializerExpressions.*;
import static java.util.Collections.emptySet;

public class SerializerGenEnum implements SerializerGen, NullableOptimization {
	private final Class<?> enumType;
	private final boolean nullable;

	public SerializerGenEnum(Class<?> enumType, boolean nullable) {
		this.enumType = enumType;
		this.nullable = nullable;
	}

	public SerializerGenEnum(Class<?> enumType) {
		this(enumType, false);
	}

	@Override
	public void accept(Visitor visitor) {
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return enumType;
	}

	@Override
	public Expression serialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		Expression ordinal = call(cast(value, Enum.class), "ordinal");
		if (isSmallEnum()) {
			ordinal = cast(ordinal, byte.class);
			return !nullable ?
					writeByte(byteArray, off, ordinal) :
					ifThenElse(isNull(value),
							writeByte(byteArray, off, value((byte) 0)),
							writeByte(byteArray, off, cast(add(ordinal, value((byte) 1)), byte.class)));
		} else {
			return !nullable ?
					writeVarInt(byteArray, off, ordinal) :
					ifThenElse(isNull(value),
							writeByte(byteArray, off, value((byte) 0)),
							writeVarInt(byteArray, off, add(ordinal, value((byte) 1))));
		}
	}

	@Override
	public Expression deserialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		return isSmallEnum() ?
				let(readByte(byteArray, off), value ->
						!nullable ?
								getArrayItem(callStatic(enumType, "values"), value) :
								ifThenElse(cmpEq(value, value((byte) 0)),
										nullRef(enumType),
										getArrayItem(callStatic(enumType, "values"), dec(value)))) :
				let(readVarInt(byteArray, off), value ->
						!nullable ?
								getArrayItem(callStatic(enumType, "values"), value) :
								ifThenElse(cmpEq(value, value(0)),
										nullRef(enumType),
										getArrayItem(callStatic(enumType, "values"), dec(value))));
	}

	private boolean isSmallEnum() {
		int size = enumType.getEnumConstants().length + (nullable ? 1 : 0);
		if (size >= 16384) throw new IllegalArgumentException();
		return size <= Byte.MAX_VALUE;
	}

	@Override
	public SerializerGen asNullable() {
		return new SerializerGenEnum(enumType, true);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenEnum that = (SerializerGenEnum) o;

		return nullable == that.nullable && enumType.equals(that.enumType);
	}

	@Override
	public int hashCode() {
		return 31 * enumType.hashCode() + (nullable ? 1 : 0);
	}
}
