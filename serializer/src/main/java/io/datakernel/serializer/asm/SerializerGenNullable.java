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
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;

@SuppressWarnings("PointlessArithmeticExpression")
public class SerializerGenNullable implements SerializerGen {
	private final SerializerGen serializer;

	public SerializerGenNullable(SerializerGen serializer) {
		this.serializer = checkNotNull(serializer);
	}

	@Override
	public void getVersions(VersionsCollector versions) {
		versions.addRecursive(serializer);
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
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		serializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		return choice(isNotNull(value),
				sequence(set(off, callStatic(SerializerUtils.class, "writeByte", byteArray, off, value((byte) 1))),
						serializer.serialize(byteArray, off, value, version, staticMethods, compatibilityLevel)),
				callStatic(SerializerUtils.class, "writeByte", byteArray, off, value((byte) 0))
		);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		serializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		Expression isNotNull = let(call(arg(0), "readByte"));
		return sequence(isNotNull, choice(cmpEq(isNotNull, value((byte) 1)),
						serializer.deserialize(serializer.getRawType(), version, staticMethods, compatibilityLevel),
						nullRef(targetType))
		);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenNullable that = (SerializerGenNullable) o;

		if (!serializer.equals(that.serializer)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return serializer.hashCode();
	}
}
