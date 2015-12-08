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
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.SerializerUtils;

import static io.datakernel.codegen.Expressions.*;

public final class SerializerGenLong extends SerializerGenPrimitive {

	private final boolean varLength;

	public SerializerGenLong() {
		super(long.class);
		this.varLength = false;
	}

	public SerializerGenLong(boolean varLength) {
		super(long.class);
		this.varLength = varLength;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenLong that = (SerializerGenLong) o;

		if (varLength != that.varLength) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = 0;
		result = 31 * result + (varLength ? 1 : 0);
		return result;
	}

	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		if (varLength) {
			return callStatic(SerializerUtils.class, "writeVarLong", byteArray, off, cast(value, long.class));
		} else {
			return callStatic(SerializerUtils.class, "writeLong", byteArray, off, cast(value, long.class));
		}
	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		if (varLength) {
			if (targetType.isPrimitive())
				return call(arg(0), "readVarLong");
			else
				return cast(call(arg(0), "readVarLong"), Long.class);
		} else {
			if (targetType.isPrimitive())
				return call(arg(0), "readLong");
			else
				return cast(call(arg(0), "readLong"), Long.class);
		}
	}
}
