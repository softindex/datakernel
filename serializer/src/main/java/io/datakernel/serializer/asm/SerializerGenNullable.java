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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;

@SuppressWarnings("PointlessArithmeticExpression")
public class SerializerGenNullable implements SerializerGen {
	private static final Logger logger = LoggerFactory.getLogger(SerializerGenNullable.class);

	public static final int VAR_ITEM = 0;
	public static final int VAR_LAST = VAR_ITEM + 1;

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

	public static void serializeNullable(SerializerGen serializer, int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> sourceType) {
		if (sourceType.isPrimitive()) {
			logger.warn("Nullable serializer for primitive type {}", sourceType);
			mv.visitVarInsn(ALOAD, varContainer);
			mv.visitInsn(ICONST_1);
			backend.writeByteGen(mv);
			serializerCaller.serialize(serializer, version, mv, locals + VAR_LAST, varContainer, sourceType);
			return;
		}

		Label notNull = new Label();
		Label exit = new Label();

		mv.visitVarInsn(ASTORE, locals + VAR_ITEM);
		mv.visitVarInsn(ALOAD, locals + VAR_ITEM);
		mv.visitJumpInsn(IFNONNULL, notNull);
		mv.visitInsn(ICONST_0);
		backend.writeByteGen(mv);
		mv.visitJumpInsn(GOTO, exit);

		mv.visitLabel(notNull);
		mv.visitInsn(ICONST_1);
		backend.writeByteGen(mv);
		mv.visitVarInsn(ALOAD, varContainer);
		mv.visitVarInsn(ALOAD, locals + VAR_ITEM);
		serializerCaller.serialize(serializer, version, mv, locals + VAR_LAST, varContainer, sourceType);

		mv.visitLabel(exit);
	}

	public static void deserializeNullable(SerializerGen serializer, int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> targetType) {
		Label notNull = new Label();
		Label exit = new Label();

		backend.readByteGen(mv);
		mv.visitJumpInsn(IFNE, notNull);

		if (targetType.isPrimitive()) {
			logger.warn("Nullable deserializer for primitive type {}", targetType);
			mv.visitTypeInsn(NEW, getInternalName(NullPointerException.class));
			mv.visitInsn(DUP);
			mv.visitMethodInsn(INVOKESPECIAL, getInternalName(NullPointerException.class),
					"<init>", getMethodDescriptor(VOID_TYPE));
			mv.visitInsn(ATHROW);
		} else {
			mv.visitInsn(ACONST_NULL);
			mv.visitJumpInsn(GOTO, exit);
		}

		mv.visitLabel(notNull);
		mv.visitVarInsn(ALOAD, varContainer);
		serializerCaller.deserialize(serializer, version, mv, locals, varContainer, targetType);

		mv.visitLabel(exit);
	}

	@Override
	public void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> sourceType) {
		serializeNullable(serializer, version, mv, backend, varContainer, locals, serializerCaller, sourceType);
	}

	@Override
	public void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> targetType) {
		deserializeNullable(serializer, version, mv, backend, varContainer, locals, serializerCaller, targetType);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenNullable that = (SerializerGenNullable) o;

		return serializer.equals(that.serializer);
	}

	@Override
	public int hashCode() {
		return serializer.hashCode();
	}
}
