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
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

import static com.google.common.base.Preconditions.checkArgument;
import static io.datakernel.serializer.asm.Utils.castSourceType;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.getMethodDescriptor;
import static org.objectweb.asm.Type.getType;

public class SerializerGenString implements SerializerGen {
	private static final int VAR_S = 0;

	private final boolean utf16;
	private final boolean nullable;
	private final int maxLength;

	public SerializerGenString(boolean utf16, boolean nullable, int maxLength) {
		checkArgument(maxLength == -1 || maxLength > 0);
		this.utf16 = utf16;
		this.nullable = nullable;
		this.maxLength = maxLength;
	}

	public SerializerGenString() {
		this(false, false, -1);
	}

	public SerializerGenString(boolean utf16, boolean nullable) {
		this(utf16, nullable, -1);
	}

	public SerializerGenString utf16(boolean utf16) {
		return new SerializerGenString(utf16, nullable, maxLength);
	}

	public SerializerGenString nullable(boolean nullable) {
		return new SerializerGenString(utf16, nullable, maxLength);
	}

	public SerializerGen maxLength(int maxLength) {
		return new SerializerGenString(utf16, nullable, maxLength);
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
		return String.class;
	}

	@Override
	public void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> sourceType) {
		castSourceType(mv, sourceType, String.class);

		if (maxLength >= 0) {
			Label lengthLess = new Label();
			mv.visitVarInsn(ASTORE, locals + VAR_S);
			mv.visitVarInsn(ALOAD, locals + VAR_S);
			mv.visitJumpInsn(IFNULL, lengthLess);
			mv.visitVarInsn(ALOAD, locals + VAR_S);
			mv.visitMethodInsn(INVOKEVIRTUAL, getType(String.class).getInternalName(), "length", getMethodDescriptor(getType(int.class)));
			mv.visitLdcInsn(maxLength);

			mv.visitJumpInsn(IF_ICMPLE, lengthLess);
			mv.visitVarInsn(ALOAD, locals + VAR_S);
			mv.visitInsn(ICONST_0);
			mv.visitLdcInsn(maxLength);
			mv.visitMethodInsn(INVOKEVIRTUAL, getType(String.class).getInternalName(), "substring", getMethodDescriptor(getType(String.class),
					getType(int.class), getType(int.class)));
			mv.visitVarInsn(ASTORE, locals + VAR_S);
			mv.visitLabel(lengthLess);
			mv.visitVarInsn(ALOAD, locals + VAR_S);
		}

		if (utf16) {
			if (nullable)
				backend.writeNullableUTF16Gen(mv);
			else
				backend.writeUTF16Gen(mv);
		} else {
			if (nullable)
				backend.writeNullableUTF8Gen(mv);
			else
				backend.writeUTF8Gen(mv);
		}
	}

	@Override
	public void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> targetType) {
		checkArgument(targetType.isAssignableFrom(String.class));

		if (utf16) {
			if (nullable)
				backend.readNullableUTF16Gen(mv);
			else
				backend.readUTF16Gen(mv);
		} else {
			if (nullable)
				backend.readNullableUTF8Gen(mv);
			else
				backend.readUTF8Gen(mv);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenString that = (SerializerGenString) o;

		return (nullable == that.nullable) && (utf16 == that.utf16);
	}

	@Override
	public int hashCode() {
		int result = (utf16 ? 1 : 0);
		result = 31 * result + (nullable ? 1 : 0);
		return result;
	}
}
