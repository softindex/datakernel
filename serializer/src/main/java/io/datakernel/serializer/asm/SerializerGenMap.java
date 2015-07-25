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

public final class SerializerGenMap implements SerializerGen {
	public static final int VAR_MAP = 0;
	public static final int VAR_LENGTH = 1;
	public static final int VAR_I = 2;
	public static final int VAR_ENTRY = 3;
	public static final int VAR_LAST = 4;

	private final SerializerGen keySerializer;
	private final SerializerGen valueSerializer;

	public SerializerGenMap(SerializerGen keySerializer, SerializerGen valueSerializer) {
		this.keySerializer = checkNotNull(keySerializer);
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
		return Map.class;
	}

	@Override
	public void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> sourceType) {
		castSourceType(mv, sourceType, Map.class);

		mv.visitVarInsn(ASTORE, locals + VAR_MAP);
		mv.visitVarInsn(ALOAD, locals + VAR_MAP);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(Map.class), "size", getMethodDescriptor(INT_TYPE));
		backend.writeVarIntGen(mv);

		mv.visitVarInsn(ALOAD, locals + VAR_MAP);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(Map.class), "entrySet", getMethodDescriptor(getType(Set.class)));
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(Set.class), "iterator", getMethodDescriptor(getType(Iterator.class)));
		mv.visitVarInsn(ASTORE, locals + VAR_I);

		Label loop = new Label();
		mv.visitLabel(loop);

		mv.visitVarInsn(ALOAD, locals + VAR_I);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(Iterator.class), "hasNext", getMethodDescriptor(BOOLEAN_TYPE));
		Label exit = new Label();
		mv.visitJumpInsn(IFEQ, exit);

		mv.visitVarInsn(ALOAD, locals + VAR_I);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(Iterator.class), "next", getMethodDescriptor(getType(Object.class)));
		mv.visitTypeInsn(CHECKCAST, getInternalName(Map.Entry.class));
		mv.visitVarInsn(ASTORE, locals + VAR_ENTRY);

		mv.visitVarInsn(ALOAD, varContainer);
		mv.visitVarInsn(ALOAD, locals + VAR_ENTRY);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(Map.Entry.class), "getKey", getMethodDescriptor(getType(Object.class)));
		mv.visitTypeInsn(CHECKCAST, getInternalName(keySerializer.getRawType()));
		serializerCaller.serialize(keySerializer, version, mv, locals + VAR_LAST, varContainer, keySerializer.getRawType());

		mv.visitVarInsn(ALOAD, varContainer);
		mv.visitVarInsn(ALOAD, locals + VAR_ENTRY);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(Map.Entry.class), "getValue", getMethodDescriptor(getType(Object.class)));
		mv.visitTypeInsn(CHECKCAST, getInternalName(valueSerializer.getRawType()));
		serializerCaller.serialize(valueSerializer, version, mv, locals + VAR_LAST, varContainer, valueSerializer.getRawType());

		mv.visitJumpInsn(GOTO, loop);

		mv.visitLabel(exit);
	}

	@Override
	public void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> targetType) {
		boolean isEnum = keySerializer.getRawType().isEnum();
		Class<?> targetInstance = isEnum ? EnumMap.class : LinkedHashMap.class;
		checkArgument(targetType.isAssignableFrom(targetInstance));

		backend.readVarIntGen(mv);
		mv.visitVarInsn(ISTORE, locals + VAR_LENGTH);

		mv.visitTypeInsn(NEW, getInternalName(targetInstance));
		mv.visitInsn(DUP);
		if (isEnum) {
			mv.visitLdcInsn(getType(keySerializer.getRawType()));
			mv.visitMethodInsn(INVOKESPECIAL, getInternalName(targetInstance), "<init>", getMethodDescriptor(VOID_TYPE, getType(Class.class)));
		} else {
			mv.visitVarInsn(ILOAD, locals + VAR_LENGTH);
			mv.visitMethodInsn(INVOKESPECIAL, getInternalName(targetInstance), "<init>", getMethodDescriptor(VOID_TYPE, INT_TYPE));
		}
		mv.visitVarInsn(ASTORE, locals + VAR_MAP);

		mv.visitInsn(ICONST_0);
		mv.visitVarInsn(ISTORE, locals + VAR_I);

		Label loop = new Label();
		mv.visitLabel(loop);
		mv.visitVarInsn(ILOAD, locals + VAR_I);
		mv.visitVarInsn(ILOAD, locals + VAR_LENGTH);
		Label exit = new Label();
		mv.visitJumpInsn(IF_ICMPGE, exit);

		mv.visitVarInsn(ALOAD, locals + VAR_MAP);

		mv.visitVarInsn(ALOAD, varContainer);
		serializerCaller.deserialize(keySerializer, version, mv, locals + VAR_LAST, varContainer, keySerializer.getRawType());
		mv.visitVarInsn(ALOAD, varContainer);
		serializerCaller.deserialize(valueSerializer, version, mv, locals + VAR_LAST, varContainer, valueSerializer.getRawType());

		mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(targetInstance), "put",
				getMethodDescriptor(getType(Object.class), getType(Object.class), getType(Object.class)));
		mv.visitInsn(POP);

		mv.visitIincInsn(locals + VAR_I, 1);
		mv.visitJumpInsn(GOTO, loop);

		mv.visitLabel(exit);

		mv.visitVarInsn(ALOAD, locals + VAR_MAP);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenMap that = (SerializerGenMap) o;

		return (keySerializer.equals(that.keySerializer)) && (valueSerializer.equals(that.valueSerializer));
	}

	@Override
	public int hashCode() {
		int result = keySerializer.hashCode();
		result = 31 * result + valueSerializer.hashCode();
		return result;
	}
}
