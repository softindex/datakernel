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
import io.datakernel.serializer.HasNullable;

import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.serializer.asm.SerializerExpressions.*;
import static java.util.Collections.emptySet;

public class SerializerDefEnum implements SerializerDef, HasNullable {
	private final Class<?> enumType;
	private final boolean nullable;

	public SerializerDefEnum(Class<?> enumType, boolean nullable) {
		this.enumType = enumType;
		this.nullable = nullable;
	}

	public SerializerDefEnum(Class<?> enumType) {
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
	public Class<?> getRawType() {
		return enumType;
	}

	@Override
	public Expression encoder(DefiningClassLoader classLoader, StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		Expression ordinal = call(cast(value, Enum.class), "ordinal");
		if (isSmallEnum()) {
			return !nullable ?
					writeByte(buf, pos, cast(ordinal, byte.class)) :
					ifThenElse(isNull(value),
							writeByte(buf, pos, value((byte) 0)),
							writeByte(buf, pos, cast(add(ordinal, value(1)), byte.class)));
		} else {
			return !nullable ?
					writeVarInt(buf, pos, ordinal) :
					ifThenElse(isNull(value),
							writeByte(buf, pos, value((byte) 0)),
							writeVarInt(buf, pos, add(ordinal, value((byte) 1))));
		}
	}

	@Override
	public Expression decoder(DefiningClassLoader classLoader, StaticDecoders staticDecoders, Expression in, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		return isSmallEnum() ?
				let(readByte(in), b ->
						!nullable ?
								getArrayItem(callStatic(enumType, "values"), b) :
								ifThenElse(cmpEq(b, value((byte) 0)),
										nullRef(enumType),
										getArrayItem(callStatic(enumType, "values"), dec(b)))) :
				let(readVarInt(in), value ->
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
	public SerializerDef withNullable() {
		return new SerializerDefEnum(enumType, true);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerDefEnum that = (SerializerDefEnum) o;

		return nullable == that.nullable && enumType.equals(that.enumType);
	}

	@Override
	public int hashCode() {
		return 31 * enumType.hashCode() + (nullable ? 1 : 0);
	}
}
