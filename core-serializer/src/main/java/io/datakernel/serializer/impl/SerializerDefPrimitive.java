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

package io.datakernel.serializer.impl;

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializerDef;

import java.util.Set;

import static io.datakernel.codegen.Expressions.cast;
import static io.datakernel.codegen.utils.Primitives.wrap;
import static io.datakernel.common.Preconditions.checkArgument;
import static java.util.Collections.emptySet;

public abstract class SerializerDefPrimitive implements SerializerDef {

	protected final Class<?> primitiveType;
	protected final Class<?> wrappedType;
	protected final boolean wrapped;

	protected SerializerDefPrimitive(Class<?> primitiveType, boolean wrapped) {
		checkArgument(primitiveType.isPrimitive(), "Not a primitive type");
		this.primitiveType = primitiveType;
		this.wrappedType = wrap(primitiveType);
		this.wrapped = wrapped;
	}

	@Override
	public void accept(Visitor visitor) {
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
	}

	@Override
	public Class<?> getEncodeType() {
		return wrapped ? wrappedType : primitiveType;
	}

	public boolean isWrapped() {
		return wrapped;
	}

	public abstract SerializerDef ensureWrapped();

	protected abstract Expression doSerialize(Expression byteArray, Variable off, Expression value, CompatibilityLevel compatibilityLevel);

	protected abstract Expression doDeserialize(Expression in, CompatibilityLevel compatibilityLevel);

	@Override
	public final Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return doSerialize(buf, pos, cast(value, primitiveType), compatibilityLevel);
	}

	@Override
	public final Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		Expression expression = doDeserialize(in, compatibilityLevel);
		return wrapped ? cast(expression, wrappedType) : expression;
	}
}
