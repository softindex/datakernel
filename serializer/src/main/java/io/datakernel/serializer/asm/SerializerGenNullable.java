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
import io.datakernel.serializer.SerializerBuilder;

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
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		serializer.prepareSerializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression serialize(Expression value, int version, SerializerBuilder.StaticMethods staticMethods) {
		return choice(ifNotNull(value),
				sequence(call(arg(0), "writeByte", value((byte) 1)),
						serializer.serialize(value, version, staticMethods),
						voidExp()),
				sequence(call(arg(0), "writeByte", value((byte) 0)), voidExp())
		);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		serializer.prepareDeserializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods) {
		Expression isNotNull = let(call(arg(0), "readByte"));
		return sequence(isNotNull, choice(cmpEq(isNotNull, value((byte) 1)),
						serializer.deserialize(serializer.getRawType(), version, staticMethods),
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
