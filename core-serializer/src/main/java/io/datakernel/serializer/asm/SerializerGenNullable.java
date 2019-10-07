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
import io.datakernel.serializer.util.BinaryOutputUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static java.util.Collections.emptySet;

public class SerializerGenNullable implements SerializerGen {
	private final SerializerGen serializer;

	public SerializerGenNullable(@NotNull SerializerGen serializer) {
		this.serializer = serializer;
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visit(serializer);
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
	}

	@Override
	public boolean isInline() {
		return serializer.isInline();
	}

	@Override
	public Class<?> getRawType() {
		return serializer.getRawType();
	}

	@Override
	public Expression serialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return ifThenElse(isNotNull(value),
				sequence(set(off, callStatic(BinaryOutputUtils.class, "writeByte", byteArray, off, value((byte) 1))),
						serializer.serialize(classLoader, byteArray, off, value, version, compatibilityLevel)),
				callStatic(BinaryOutputUtils.class, "writeByte", byteArray, off, value((byte) 0))
		);
	}

	@Override
	public Expression deserialize(DefiningClassLoader classLoader, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		return let(call(arg(0), "readByte"),
				isNotNull -> ifThenElse(cmpEq(isNotNull, value((byte) 1)),
						serializer.deserialize(classLoader, serializer.getRawType(), version, compatibilityLevel),
						nullRef(targetType)));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenNullable that = (SerializerGenNullable) o;

		return serializer.equals(that.serializer);
	}

	@Override
	public int hashCode() {
		return serializer.hashCode();
	}
}
