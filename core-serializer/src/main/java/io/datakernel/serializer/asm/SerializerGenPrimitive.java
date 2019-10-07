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

import java.util.Set;

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
