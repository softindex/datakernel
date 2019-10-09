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

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.util.BinaryOutputUtils2;

import static io.datakernel.codegen.Expressions.*;

public final class SerializerGenInt extends SerializerGenPrimitive {

	private final boolean varLength;

	public SerializerGenInt() {
		super(int.class);
		this.varLength = false;
	}

	public SerializerGenInt(boolean varLength) {
		super(int.class);
		this.varLength = varLength;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenInt that = (SerializerGenInt) o;

		return varLength == that.varLength;
	}

	@Override
	public int hashCode() {
		int result = 0;
		result = 31 * result + (varLength ? 1 : 0);
		return result;
	}

	@Override
	public Expression serialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		if (varLength) {
			return BinaryOutputUtils2.writeVarInt(byteArray, off, cast(value, int.class));
		} else {
			return BinaryOutputUtils2.writeInt(byteArray, off, cast(value, int.class));
		}
	}

	@Override
	public Expression deserialize(DefiningClassLoader classLoader, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		if (varLength) {
			if (targetType.isPrimitive())
				return call(arg(0), "readVarInt");
			else
				return cast(call(arg(0), "readVarInt"), Integer.class);
		} else {
			if (targetType.isPrimitive())
				return call(arg(0), "readInt");
			else
				return cast(call(arg(0), "readInt"), Integer.class);
		}
	}
}
