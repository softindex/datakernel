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

import io.datakernel.serializer.SerializationInputBuffer;
import io.datakernel.serializer.SerializerCaller;
import org.objectweb.asm.MethodVisitor;

import java.nio.ByteBuffer;

import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;

public class SerializerGenByteBuffer implements SerializerGen {
	private static final int VAR_BYTEBUFFER = 0;
	private static final int VAR_ARRAY = 1;
	private static final int VAR_ARRAY_OFFSET = 2;
	private static final int VAR_ARRAY_LENGTH = 3;
	private final boolean wrapped;

	public SerializerGenByteBuffer() {
		this(false);
	}

	public SerializerGenByteBuffer(boolean wrapped) {
		this.wrapped = wrapped;
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
		return ByteBuffer.class;
	}

	@Override
	public void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller,
	                      Class<?> sourceType) {
		mv.visitVarInsn(ASTORE, locals + VAR_BYTEBUFFER);
		mv.visitVarInsn(ALOAD, locals + VAR_BYTEBUFFER);
		mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(ByteBuffer.class), "array", getMethodDescriptor(getType(byte[].class)));
		mv.visitVarInsn(ASTORE, locals + VAR_ARRAY);

		mv.visitVarInsn(ALOAD, locals + VAR_BYTEBUFFER);
		mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(ByteBuffer.class), "position", getMethodDescriptor(INT_TYPE));
		mv.visitVarInsn(ISTORE, locals + VAR_ARRAY_OFFSET);

		mv.visitVarInsn(ALOAD, locals + VAR_BYTEBUFFER);
		mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(ByteBuffer.class), "remaining", getMethodDescriptor(INT_TYPE));
		mv.visitVarInsn(ISTORE, locals + VAR_ARRAY_LENGTH);

		mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_LENGTH);
		backend.writeVarIntGen(mv);

		mv.visitVarInsn(ALOAD, varContainer);
		mv.visitVarInsn(ALOAD, locals + VAR_ARRAY);
		mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_OFFSET);
		mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_LENGTH);
		backend.writeBytesGen(mv);
	}

	@Override
	public void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller,
	                        Class<?> targetType) {
		backend.readVarIntGen(mv);
		mv.visitVarInsn(ISTORE, locals + VAR_ARRAY_LENGTH);
		if (wrapped) {
			mv.visitVarInsn(ALOAD, varContainer);
			mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(SerializationInputBuffer.class), "array", getMethodDescriptor(getType(byte[].class)));
			mv.visitVarInsn(ASTORE, locals + VAR_ARRAY);

			mv.visitVarInsn(ALOAD, varContainer);
			mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(SerializationInputBuffer.class), "position", getMethodDescriptor(INT_TYPE));
			mv.visitVarInsn(ISTORE, locals + VAR_ARRAY_OFFSET);

			mv.visitVarInsn(ALOAD, varContainer);
			mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_OFFSET);
			mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_LENGTH);
			mv.visitInsn(IADD);
			mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(SerializationInputBuffer.class), "position", getMethodDescriptor(VOID_TYPE, INT_TYPE));

			mv.visitVarInsn(ALOAD, locals + VAR_ARRAY);
			mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_OFFSET);
			mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_LENGTH);
			mv.visitMethodInsn(INVOKESTATIC, getInternalName(ByteBuffer.class), "wrap", getMethodDescriptor(getType(ByteBuffer.class), getType(byte[].class),
					getType(int.class), getType(int.class)));
		} else {
			mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_LENGTH);
			mv.visitMethodInsn(INVOKESTATIC, getInternalName(ByteBuffer.class), "allocate", getMethodDescriptor(getType(ByteBuffer.class), getType(int.class)));
			mv.visitVarInsn(ASTORE, locals + VAR_BYTEBUFFER);
			mv.visitVarInsn(ALOAD, varContainer);
			mv.visitVarInsn(ALOAD, locals + VAR_BYTEBUFFER);
			mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(ByteBuffer.class), "array", getMethodDescriptor(getType(byte[].class)));
			mv.visitInsn(ICONST_0);
			mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_LENGTH);
			backend.readBytesGen(mv);
			mv.visitVarInsn(ALOAD, locals + VAR_BYTEBUFFER);
		}
	}
}
