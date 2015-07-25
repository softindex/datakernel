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

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.serializer.asm.Utils.castSourceType;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;

public final class SerializerGenSet implements SerializerGen {
	public static final int VAR_SET = 0;
	public static final int VAR_LENGTH = 1;
	public static final int VAR_I = 2;
	public static final int VAR_LAST = 3;

	private final SerializerGen valueSerializer;

	public SerializerGenSet(SerializerGen valueSerializer) {
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
		return Set.class;
	}

	@Override
	public void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller,
	                      Class<?> sourceType) {
		castSourceType(mv, sourceType, Set.class);

		mv.visitVarInsn(ASTORE, locals + VAR_SET);
		mv.visitVarInsn(ALOAD, locals + VAR_SET);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(Set.class), "size", getMethodDescriptor(INT_TYPE));
		backend.writeVarIntGen(mv);

		mv.visitVarInsn(ALOAD, locals + VAR_SET);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(Set.class), "iterator", getMethodDescriptor(getType(Iterator.class)));
		mv.visitVarInsn(ASTORE, locals + VAR_I);

		Label loop = new Label();
		mv.visitLabel(loop);

		mv.visitVarInsn(ALOAD, locals + VAR_I);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(Iterator.class), "hasNext", getMethodDescriptor(BOOLEAN_TYPE));
		Label exit = new Label();
		mv.visitJumpInsn(IFEQ, exit);

		mv.visitVarInsn(ALOAD, varContainer);
		mv.visitVarInsn(ALOAD, locals + VAR_I);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(Iterator.class), "next", getMethodDescriptor(getType(Object.class)));
		mv.visitTypeInsn(CHECKCAST, getInternalName(valueSerializer.getRawType()));
		serializerCaller.serialize(valueSerializer, version, mv, locals + VAR_LAST, varContainer, valueSerializer.getRawType());

		mv.visitJumpInsn(GOTO, loop);

		mv.visitLabel(exit);
	}

	@Override
	public void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller,
	                        Class<?> targetType) {
		boolean isEnum = valueSerializer.getRawType().isEnum();
		Class<?> targetInstance = isEnum ? EnumSet.class : LinkedHashSet.class;
		checkArgument(targetType.isAssignableFrom(targetInstance));

		backend.readVarIntGen(mv);
		mv.visitVarInsn(ISTORE, locals + VAR_LENGTH);

		if (isEnum) {
			deserializeEnumSet(version, mv, varContainer, locals, serializerCaller);
		} else {
			deserializeHashSet(version, mv, varContainer, locals, serializerCaller);
		}
	}

	private void deserializeEnumSet(int version, MethodVisitor mv, int varContainer, int locals, SerializerCaller serializerCaller) {
		mv.visitVarInsn(ILOAD, locals + VAR_LENGTH);
		mv.visitTypeInsn(ANEWARRAY, getInternalName(Object.class)); // TODO: max length check
		mv.visitVarInsn(ASTORE, locals + VAR_SET);

		mv.visitInsn(ICONST_0);
		mv.visitVarInsn(ISTORE, locals + VAR_I);
		Label loop = new Label();
		mv.visitLabel(loop);
		mv.visitVarInsn(ILOAD, locals + VAR_I);
		mv.visitVarInsn(ILOAD, locals + VAR_LENGTH);
		Label exit = new Label();
		mv.visitJumpInsn(IF_ICMPGE, exit);
		mv.visitVarInsn(ALOAD, locals + VAR_SET);
		mv.visitVarInsn(ILOAD, locals + VAR_I);
		mv.visitVarInsn(ALOAD, varContainer);
		serializerCaller.deserialize(valueSerializer, version, mv, locals + VAR_LAST, varContainer, valueSerializer.getRawType());
		mv.visitInsn(AASTORE);
		mv.visitIincInsn(locals + VAR_I, 1);
		mv.visitJumpInsn(GOTO, loop);
		mv.visitLabel(exit);

		mv.visitVarInsn(ALOAD, locals + VAR_SET);
		mv.visitInsn(DUP);
		mv.visitMethodInsn(INVOKESTATIC, getInternalName(Arrays.class), "asList", getMethodDescriptor(getType(List.class), getType(Object[].class)));
		mv.visitMethodInsn(INVOKESTATIC, getInternalName(EnumSet.class), "copyOf", getMethodDescriptor(getType(EnumSet.class), getType(Collection.class)));
	}

	private void deserializeHashSet(int version, MethodVisitor mv, int varContainer, int locals, SerializerCaller serializerCaller) {
		mv.visitTypeInsn(NEW, getInternalName(LinkedHashSet.class));
		mv.visitInsn(DUP);
		mv.visitVarInsn(ILOAD, locals + VAR_LENGTH);
		mv.visitMethodInsn(INVOKESPECIAL, getInternalName(LinkedHashSet.class), "<init>", getMethodDescriptor(VOID_TYPE, INT_TYPE));
		mv.visitVarInsn(ASTORE, locals + VAR_SET);

		mv.visitInsn(ICONST_0);
		mv.visitVarInsn(ISTORE, locals + VAR_I);

		Label loop = new Label();
		mv.visitLabel(loop);
		mv.visitVarInsn(ILOAD, locals + VAR_I);
		mv.visitVarInsn(ILOAD, locals + VAR_LENGTH);
		Label exit = new Label();
		mv.visitJumpInsn(IF_ICMPGE, exit);

		mv.visitVarInsn(ALOAD, locals + VAR_SET);

		mv.visitVarInsn(ALOAD, varContainer);
		serializerCaller.deserialize(valueSerializer, version, mv, locals + VAR_LAST, varContainer, valueSerializer.getRawType());
		mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(LinkedHashSet.class), "add", getMethodDescriptor(BOOLEAN_TYPE, getType(Object.class)));
		mv.visitInsn(POP);

		mv.visitIincInsn(locals + VAR_I, 1);
		mv.visitJumpInsn(GOTO, loop);

		mv.visitLabel(exit);

		mv.visitVarInsn(ALOAD, locals + VAR_SET);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		SerializerGenSet that = (SerializerGenSet) o;

		return valueSerializer.equals(that.valueSerializer);
	}

	@Override
	public int hashCode() {
		return valueSerializer.hashCode();
	}
}
