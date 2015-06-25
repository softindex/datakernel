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

import io.datakernel.serializer.SerializerCaller;
import org.objectweb.asm.MethodVisitor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.primitives.Primitives.wrap;
import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Type.getInternalName;

public abstract class SerializerGenPrimitive implements SerializerGen {

	private final Class<?> primitiveType;

	protected SerializerGenPrimitive(Class<?> primitiveType) {
		checkArgument(primitiveType.isPrimitive());
		this.primitiveType = primitiveType;
	}

	@Override
	public final void getVersions(VersionsCollector versions) {
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

	protected abstract void doSerialize(MethodVisitor mv, SerializerBackend backend);

	@Override
	public final void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> sourceType) {
		if (sourceType.isPrimitive()) {
			checkArgument(sourceType == getPrimitiveType());
		} else {
			if (sourceType != getBoxedType()) {
				checkArgument(sourceType.isAssignableFrom(getBoxedType()));
				mv.visitTypeInsn(CHECKCAST, getInternalName(getBoxedType()));
			}
			Utils.unbox(mv, getPrimitiveType());
		}
		doSerialize(mv, backend);
	}

	protected abstract void doDeserialize(MethodVisitor mv, SerializerBackend backend);

	@Override
	public final void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer,
	                              int locals, SerializerCaller serializerCaller, Class<?> targetType) {
		doDeserialize(mv, backend);
		if (targetType.isPrimitive()) {
			checkArgument(targetType == getPrimitiveType());
		} else {
			checkArgument(targetType.isAssignableFrom(getBoxedType()));
			Utils.box(mv, getPrimitiveType());
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return 0;
	}

}
