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

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.serializer.asm.Utils.castSourceType;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.getInternalName;

@SuppressWarnings("PointlessArithmeticExpression")
public final class SerializerGenList implements SerializerGen {
	public static final int VAR_LIST = 0;
	public static final int VAR_LENGTH = 1;
	public static final int VAR_I = 2;
	public static final int VAR_LAST = VAR_I + 1;

	private final SerializerGen valueSerializer;

	public SerializerGenList(SerializerGen valueSerializer) {
		this.valueSerializer = checkNotNull(valueSerializer);
	}

	@Override
	public void getVersions(VersionsCollector versions) {
		versions.addRecursive(valueSerializer);
	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return List.class;
	}

	@Override
	public void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> sourceType) {
		castSourceType(mv, sourceType, List.class);

		mv.visitInsn(DUP);
		mv.visitVarInsn(ASTORE, locals + VAR_LIST);
		mv.visitMethodInsn(INVOKEINTERFACE, "java/util/List", "size", "()I");

		mv.visitInsn(DUP);
		mv.visitVarInsn(ISTORE, locals + VAR_LENGTH);
		backend.writeVarIntGen(mv);

		mv.visitInsn(ICONST_0);
		mv.visitVarInsn(ISTORE, locals + VAR_I);
		Label loop = new Label();
		mv.visitLabel(loop);
		mv.visitVarInsn(ILOAD, locals + VAR_I);
		mv.visitVarInsn(ILOAD, locals + VAR_LENGTH);
		Label exit = new Label();
		mv.visitJumpInsn(IF_ICMPGE, exit);
		mv.visitVarInsn(ALOAD, varContainer);
		mv.visitVarInsn(ALOAD, locals + VAR_LIST);
		mv.visitVarInsn(ILOAD, locals + VAR_I);
		mv.visitMethodInsn(INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;");
		serializerCaller.serialize(valueSerializer, version, mv, locals + VAR_LAST, varContainer, Object.class);
		mv.visitIincInsn(locals + VAR_I, 1);
		mv.visitJumpInsn(GOTO, loop);
		mv.visitLabel(exit);
	}

	@Override
	public void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> targetType) {
		checkArgument(targetType.isAssignableFrom(List.class));

		backend.readVarIntGen(mv);
		mv.visitInsn(DUP);
		mv.visitVarInsn(ISTORE, locals + VAR_LENGTH);
		mv.visitTypeInsn(ANEWARRAY, getInternalName(Object.class)); // TODO (vsavchuk): max length check
		mv.visitVarInsn(ASTORE, locals + VAR_LIST);

		mv.visitInsn(ICONST_0);
		mv.visitVarInsn(ISTORE, locals + VAR_I);
		Label loop = new Label();
		mv.visitLabel(loop);
		mv.visitVarInsn(ILOAD, locals + VAR_I);
		mv.visitVarInsn(ILOAD, locals + VAR_LENGTH);
		Label exit = new Label();
		mv.visitJumpInsn(IF_ICMPGE, exit);
		mv.visitVarInsn(ALOAD, locals + VAR_LIST);
		mv.visitVarInsn(ILOAD, locals + VAR_I);
		mv.visitVarInsn(ALOAD, varContainer);
		serializerCaller.deserialize(valueSerializer, version, mv, locals + VAR_LAST, varContainer, Object.class);
		mv.visitInsn(AASTORE);
		mv.visitIincInsn(locals + VAR_I, 1);
		mv.visitJumpInsn(GOTO, loop);
		mv.visitLabel(exit);

		mv.visitVarInsn(ALOAD, locals + VAR_LIST);
		mv.visitMethodInsn(INVOKESTATIC, "java/util/Arrays", "asList", "([Ljava/lang/Object;)Ljava/util/List;");
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenList that = (SerializerGenList) o;

		return valueSerializer.equals(that.valueSerializer);
	}

	@Override
	public int hashCode() {
		return valueSerializer.hashCode();
	}
}
