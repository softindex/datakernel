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

import io.datakernel.bytebuf.SerializationUtils;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.SerializerBuilder.StaticMethods;
import org.objectweb.asm.Type;

import static io.datakernel.codegen.Expressions.*;

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
	public void getVersions(VersionsCollector versions) {
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
	public void prepareSerializeStaticMethods(int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		Expression ordinal = call(cast(value, Enum.class), "ordinal");
		if (isSmallEnum()) {
			ordinal = cast(ordinal, Type.BYTE_TYPE);
			return !nullable ?
					callStatic(SerializationUtils.class, "writeByte", byteArray, off, ordinal) :
					ifThenElse(isNull(value),
							callStatic(SerializationUtils.class, "writeByte", byteArray, off, value((byte) 0)),
							callStatic(SerializationUtils.class, "writeByte", byteArray, off, cast(add(ordinal, value((byte) 1)), Type.BYTE_TYPE)));
		}
		return !nullable ?
				callStatic(SerializationUtils.class, "writeVarInt", byteArray, off, ordinal) :
				ifThenElse(isNull(value),
						callStatic(SerializationUtils.class, "writeVarInt", byteArray, off, value(0)),
						callStatic(SerializationUtils.class, "writeVarInt", byteArray, off, inc(ordinal)));
	}

	@Override
	public void prepareDeserializeStaticMethods(int version,
			StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@Override
	public Expression deserialize(Class<?> targetType, int version,
			StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		if (isSmallEnum()) {
			Variable value = let(call(arg(0), "readByte"));
			return !nullable ?
					getArrayItem(callStatic(enumType, "values"), value) :
					ifThenElse(cmpEq(value, value((byte) 0)),
							nullRef(enumType),
							getArrayItem(callStatic(enumType, "values"), dec(value)));
		}
		Variable value = let(call(arg(0), "readVarInt"));
		return !nullable ?
				getArrayItem(callStatic(enumType, "values"), value) :
				ifThenElse(cmpEq(value, value(0)),
						nullRef(enumType),
						getArrayItem(callStatic(enumType, "values"), dec(value)));
	}

	private boolean isSmallEnum() {
		return enumType.getEnumConstants().length <= Byte.MAX_VALUE - (nullable ? 1 : 0);
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
