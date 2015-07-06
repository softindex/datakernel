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

import static com.google.common.base.Preconditions.checkArgument;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;

import io.datakernel.serializer.SerializerCaller;
import org.objectweb.asm.MethodVisitor;

@SuppressWarnings("StatementWithEmptyBody")
public class SerializerGenEnum implements SerializerGen {
	private static final int VAR_I = 1;
	private final Class<?> enumType;

	public SerializerGenEnum(Class<?> enumType) {
		this.enumType = enumType;
	}

	@Override
	public void getVersions(VersionsCollector versions) {
	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return enumType;
	}

	@Override
	public void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> sourceType) {
		if (sourceType.isEnum()) {
			mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(Enum.class), "ordinal", getMethodDescriptor(INT_TYPE));
		} else if (sourceType == byte.class) {
			// do nothing
		} else if (sourceType == Byte.class) {
			Utils.unbox(mv, byte.class);
		} else
			throw new IllegalArgumentException("Unrelated type " + sourceType);
		backend.writeByteGen(mv);
	}

	@Override
	public void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> targetType) {
		backend.readByteGen(mv);
		if (targetType.isEnum()) {
			checkArgument(targetType.getEnumConstants().length <= Byte.MAX_VALUE);
			mv.visitVarInsn(ISTORE, locals + VAR_I);
			mv.visitMethodInsn(INVOKESTATIC, getInternalName(targetType), "values", "()[" + getDescriptor(targetType));
			mv.visitVarInsn(ILOAD, locals + VAR_I);
			mv.visitInsn(AALOAD);
		} else if (targetType == byte.class) {
			// do nothing
		} else if (targetType == Byte.class) {
			Utils.box(mv, byte.class);
		} else
			throw new IllegalArgumentException("Unrelated type " + targetType);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

//        SerializerGenEnum that = (SerializerGenEnum) o;

		return true;
	}

	@Override
	public int hashCode() {
		return 0;
	}

}
