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
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import static com.google.common.base.Preconditions.*;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.getInternalName;
import static org.objectweb.asm.Type.getType;

@SuppressWarnings("PointlessArithmeticExpression")
public final class SerializerGenArray implements SerializerGen {
	public static final int VAR_ARRAY = 0;
	public static final int VAR_ARRAY_LENGTH = 1;
	public static final int VAR_I = 2;
	public static final int VAR_LAST = VAR_I + 1;
	private final SerializerGen valueSerializer;
	private final int fixedSize;

	public SerializerGenArray(SerializerGen serializer, int fixedSize) {
		this.valueSerializer = checkNotNull(serializer);
		this.fixedSize = fixedSize;
	}

	public SerializerGenArray(SerializerGen serializer) {
		this(serializer, -1);
	}

	public SerializerGenArray fixedSize(int fixedSize) {
		return new SerializerGenArray(valueSerializer, fixedSize);
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
		return Object.class;
	}

	private void newArray(MethodVisitor mv, Class<?> type) {
		int typ;
		switch (getType(type).getSort()) {
			case Type.BOOLEAN:
				typ = Opcodes.T_BOOLEAN;
				break;
			case Type.CHAR:
				typ = Opcodes.T_CHAR;
				break;
			case Type.BYTE:
				typ = Opcodes.T_BYTE;
				break;
			case Type.SHORT:
				typ = Opcodes.T_SHORT;
				break;
			case Type.INT:
				typ = Opcodes.T_INT;
				break;
			case Type.FLOAT:
				typ = Opcodes.T_FLOAT;
				break;
			case Type.LONG:
				typ = Opcodes.T_LONG;
				break;
			case Type.DOUBLE:
				typ = Opcodes.T_DOUBLE;
				break;
			default:
				mv.visitTypeInsn(ANEWARRAY, getInternalName(type));
				return;
		}
		mv.visitIntInsn(Opcodes.NEWARRAY, typ);
	}

	@Override
	public void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> sourceType) {
		checkArgument(sourceType.isArray());
		Class<?> componentType = sourceType.getComponentType();

		mv.visitVarInsn(ASTORE, locals + VAR_ARRAY);
		if (fixedSize != -1) {
			mv.visitInsn(POP);
			mv.visitLdcInsn(fixedSize);
			mv.visitVarInsn(ISTORE, locals + VAR_ARRAY_LENGTH);
		} else {
			mv.visitVarInsn(ALOAD, locals + VAR_ARRAY);
			mv.visitInsn(ARRAYLENGTH);
			mv.visitVarInsn(ISTORE, locals + VAR_ARRAY_LENGTH);
			mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_LENGTH);
			backend.writeVarIntGen(mv);
		}

		if (componentType == Byte.TYPE) {
			checkState(valueSerializer instanceof SerializerGenByte);
			mv.visitVarInsn(ALOAD, varContainer);
			mv.visitVarInsn(ALOAD, locals + VAR_ARRAY);
			mv.visitInsn(ICONST_0);
			mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_LENGTH);
			backend.writeBytesGen(mv);
		} else {
			mv.visitInsn(ICONST_0);
			mv.visitVarInsn(ISTORE, locals + VAR_I);
			Label loop = new Label();
			mv.visitLabel(loop);
			mv.visitVarInsn(ILOAD, locals + VAR_I);
			mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_LENGTH);
			Label exit = new Label();
			mv.visitJumpInsn(IF_ICMPGE, exit);
			mv.visitVarInsn(ALOAD, varContainer);
			mv.visitVarInsn(ALOAD, locals + VAR_ARRAY);
			mv.visitVarInsn(ILOAD, locals + VAR_I);
			mv.visitInsn(getType(componentType).getOpcode(IALOAD));
			serializerCaller.serialize(valueSerializer, version, mv, locals + VAR_LAST, varContainer, componentType);
			mv.visitIincInsn(locals + VAR_I, 1);
			mv.visitJumpInsn(GOTO, loop);
			mv.visitLabel(exit);
		}
	}

	@Override
	public void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> targetType) {
		checkArgument(targetType.isArray());
		Class<?> componentType = targetType.getComponentType();

		if (fixedSize != -1) {
			mv.visitInsn(POP);
			mv.visitLdcInsn(fixedSize);
			mv.visitVarInsn(ISTORE, locals + VAR_ARRAY_LENGTH);
		} else {
			backend.readVarIntGen(mv);
			mv.visitVarInsn(ISTORE, locals + VAR_ARRAY_LENGTH);
		}

		if (componentType == Byte.TYPE) {
			checkState(valueSerializer instanceof SerializerGenByte);
			mv.visitVarInsn(ALOAD, varContainer);
			mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_LENGTH);
			mv.visitIntInsn(NEWARRAY, T_BYTE); // TODO (vsavchuk): max array size check
			mv.visitVarInsn(ASTORE, locals + VAR_ARRAY);
			mv.visitVarInsn(ALOAD, locals + VAR_ARRAY);
			mv.visitInsn(ICONST_0);
			mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_LENGTH);
			backend.readBytesGen(mv);
			mv.visitVarInsn(ALOAD, locals + VAR_ARRAY);
		} else {
			mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_LENGTH);
			newArray(mv, componentType); // TODO (vsavchuk): max array size check
			mv.visitVarInsn(ASTORE, locals + VAR_ARRAY);
			mv.visitInsn(ICONST_0);
			mv.visitVarInsn(ISTORE, locals + VAR_I);
			Label loop = new Label();
			mv.visitLabel(loop);
			mv.visitVarInsn(ILOAD, locals + VAR_I);
			mv.visitVarInsn(ILOAD, locals + VAR_ARRAY_LENGTH);
			Label exit = new Label();
			mv.visitJumpInsn(IF_ICMPGE, exit);
			mv.visitVarInsn(ALOAD, locals + VAR_ARRAY);
			mv.visitVarInsn(ILOAD, locals + VAR_I);
			mv.visitVarInsn(ALOAD, varContainer);
			serializerCaller.deserialize(valueSerializer, version, mv, locals + VAR_LAST, varContainer, componentType);
			mv.visitInsn(getType(componentType).getOpcode(IASTORE));
			mv.visitIincInsn(locals + VAR_I, 1);
			mv.visitJumpInsn(GOTO, loop);
			mv.visitLabel(exit);
			mv.visitVarInsn(ALOAD, locals + VAR_ARRAY);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenArray that = (SerializerGenArray) o;

		return (fixedSize == that.fixedSize) && (valueSerializer.equals(that.valueSerializer));
	}

	@Override
	public int hashCode() {
		int result = valueSerializer.hashCode();
		result = 31 * result + fixedSize;
		return result;
	}
}
