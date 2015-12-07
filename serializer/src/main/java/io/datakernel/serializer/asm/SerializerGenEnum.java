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
import io.datakernel.codegen.ExpressionLet;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializationOutputBuffer;
import io.datakernel.serializer.SerializerBuilder;
import org.objectweb.asm.Type;

import static io.datakernel.codegen.Expressions.*;

@SuppressWarnings("StatementWithEmptyBody")
public class SerializerGenEnum implements SerializerGen {
	private final Class<?> nameOfEnum;
	private final boolean nullable;

	public SerializerGenEnum(Class<?> c) {
		this.nameOfEnum = c;
		this.nullable = false;
	}

	public SerializerGenEnum(Class<?> c, boolean nullable) {
		this.nameOfEnum = c;
		this.nullable = nullable;
	}

	public SerializerGenEnum nullable(boolean nullable) {
		return new SerializerGenEnum(nameOfEnum, nullable);
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
		return nameOfEnum;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		Expression ordinal = cast(call(cast(value, Enum.class), "ordinal"), Type.BYTE_TYPE);
		if (!nullable) {
			return callStatic(SerializationOutputBuffer.class, "writeByte", byteArray, off, ordinal);
		} else {
			return choice(isNull(value),
					callStatic(SerializationOutputBuffer.class, "writeByte", byteArray, off, value((byte) 0)),
					callStatic(SerializationOutputBuffer.class, "writeByte", byteArray, off, cast(add(ordinal, value((byte) 1)), Type.BYTE_TYPE))
			);
		}
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		ExpressionLet value = let(call(arg(0), "readByte"));
		if (!nullable) {
			return getArrayItem(callStatic(nameOfEnum, "values"), value);
		} else {
			return choice(cmpEq(value, value((byte) 0)),
					nullRef(nameOfEnum),
					getArrayItem(callStatic(nameOfEnum, "values"), sub(value, value(1))));
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

//        SerializerGenEnum that = (SerializerGenEnum) o;

		return true;
	}

	@Override
	public int hashCode() {
		return 0;
	}

}
