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

import java.net.InetAddress;

import static com.google.common.base.Preconditions.checkArgument;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;

@SuppressWarnings("PointlessArithmeticExpression")
public class SerializerGenInetAddress implements SerializerGen {
	private static final SerializerGenInetAddress INSTANCE = new SerializerGenInetAddress();

	public static SerializerGenInetAddress instance() {
		return INSTANCE;
	}

	private SerializerGenInetAddress() {
	}

	private static final int VAR_ARRAY = 0;

	@Override
	public void getVersions(VersionsCollector versions) {
	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return InetAddress.class;
	}

	@Override
	public void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> sourceType) {
		Utils.castSourceType(mv, sourceType, InetAddress.class);

		mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(InetAddress.class), "getAddress", getMethodDescriptor(getType(byte[].class)));
		mv.visitInsn(ICONST_0);
		mv.visitLdcInsn(4);
		backend.writeBytesGen(mv);
	}

	@Override
	public void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> targetType) {
		checkArgument(targetType.isAssignableFrom(InetAddress.class));

		mv.visitIntInsn(BIPUSH, 4);
		mv.visitIntInsn(NEWARRAY, T_BYTE);
		mv.visitVarInsn(ASTORE, locals + VAR_ARRAY);
		mv.visitVarInsn(ALOAD, locals + VAR_ARRAY);
		mv.visitInsn(ICONST_0);
		mv.visitLdcInsn(4);
		backend.readBytesGen(mv);
		mv.visitVarInsn(ALOAD, locals + VAR_ARRAY);
		mv.visitMethodInsn(INVOKESTATIC, getInternalName(InetAddress.class), "getByAddress", getMethodDescriptor(getType(InetAddress.class), getType(byte[].class)));
	}
}
