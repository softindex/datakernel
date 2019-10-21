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
import org.jetbrains.annotations.NotNull;

import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.serializer.asm.SerializerExpressions.readByte;
import static io.datakernel.serializer.asm.SerializerExpressions.writeByte;
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
	public Class<?> getRawType() {
		return serializer.getRawType();
	}

	@Override
	public Expression serialize(DefiningClassLoader classLoader, StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return ifThenElse(isNotNull(value),
				sequence(
						writeByte(buf, pos, value((byte) 1)),
						serializer.serialize(classLoader, staticEncoders, buf, pos, value, version, compatibilityLevel)),
				writeByte(buf, pos, value((byte) 0))
		);
	}

	@Override
	public Expression deserialize(DefiningClassLoader classLoader, StaticDecoders staticDecoders, Expression in, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		return let(readByte(in),
				isNotNull -> ifThenElse(cmpNe(isNotNull, value((byte) 0)),
						serializer.deserialize(classLoader, staticDecoders, in, serializer.getRawType(), version, compatibilityLevel),
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
