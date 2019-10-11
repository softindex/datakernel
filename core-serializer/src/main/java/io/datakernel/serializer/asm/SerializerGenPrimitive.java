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

import java.util.Set;

import static io.datakernel.codegen.Expressions.cast;
import static io.datakernel.codegen.utils.Primitives.wrap;
import static io.datakernel.common.Preconditions.checkArgument;
import static java.util.Collections.emptySet;

public abstract class SerializerGenPrimitive implements SerializerGen {

	private final Class<?> primitiveType;

	protected SerializerGenPrimitive(Class<?> primitiveType) {
		checkArgument(primitiveType.isPrimitive(), "Not a primitive type");
		this.primitiveType = primitiveType;
	}

	@Override
	public void accept(Visitor visitor) {
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
	}

	@Override
	public final boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return getBoxedType();
	}

	public final Class<?> getPrimitiveType() {
		return primitiveType;
	}

	public final Class<?> getBoxedType() {
		return wrap(primitiveType);
	}

	protected abstract Expression doSerialize(Expression byteArray, Variable off, Expression value);

	protected abstract Expression doDeserialize(Expression byteArray, Variable off);

	@Override
	public final Expression serialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return doSerialize(byteArray, off, cast(value, primitiveType));
	}

	@Override
	public final Expression deserialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		Expression expression = doDeserialize(byteArray, off);
		return targetType.isPrimitive() ? expression : cast(expression, getBoxedType());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		return o != null && getClass() == o.getClass();
	}

	@Override
	public int hashCode() {
		return 0;
	}
}
