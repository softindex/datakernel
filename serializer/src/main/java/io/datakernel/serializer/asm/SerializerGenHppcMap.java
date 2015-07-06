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

import static com.google.common.base.CaseFormat.*;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;

import java.util.Iterator;

import com.google.common.collect.ImmutableMap;
import io.datakernel.serializer.SerializerCaller;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

@SuppressWarnings("PointlessArithmeticExpression")
public final class SerializerGenHppcMap implements SerializerGen {
	private static final int VAR_MAP = 0;
	private static final int VAR_I = 1;
	private static final int VAR_CURSOR = 2;
	private static final int VAR_LENGTH = 3;
	private static final int VAR_LAST = 4;

	private static ImmutableMap<Class<?>, SerializerGen> primitiveSerializers = ImmutableMap.<Class<?>, SerializerGen>builder()
			.put(byte.class, new SerializerGenByte())
			.put(short.class, new SerializerGenShort())
			.put(int.class, new SerializerGenInt(true))
			.put(long.class, new SerializerGenLong(false))
			.put(float.class, new SerializerGenFloat())
			.put(double.class, new SerializerGenDouble())
			.put(char.class, new SerializerGenChar())
			.build();

	private final Class<?> mapType;
	private final Class<?> hashMapType;
	private final Class<?> iteratorType;
	private final Class<?> keyType;
	private final Class<?> valueType;
	private final SerializerGen keySerializer;
	private final SerializerGen valueSerializer;

	public SerializerGenHppcMap(Class<?> mapType, Class<?> keyType, Class<?> valueType) {
		this.mapType = mapType;
		this.keyType = keyType;
		this.valueType = valueType;
		this.keySerializer = checkNotNull(primitiveSerializers.get(keyType));
		this.valueSerializer = checkNotNull(primitiveSerializers.get(valueType));
		try {
			this.iteratorType = Class.forName("com.carrotsearch.hppc.cursors." +
					LOWER_CAMEL.to(UPPER_CAMEL, keyType.getName()) + LOWER_CAMEL.to(UPPER_CAMEL, valueType.getName()) + "Cursor");
			this.hashMapType = Class.forName("com.carrotsearch.hppc." +
					LOWER_CAMEL.to(UPPER_CAMEL, keyType.getName()) + LOWER_CAMEL.to(UPPER_CAMEL, valueType.getName()) + "OpenHashMap");
		} catch (ClassNotFoundException e) {
			throw propagate(e);
		}
	}

	@Override
	public void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller,
	                      Class<?> sourceType) {
		mv.visitVarInsn(ASTORE, locals + VAR_MAP);
		mv.visitVarInsn(ALOAD, locals + VAR_MAP);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(mapType), "size", getMethodDescriptor(INT_TYPE));
		backend.writeVarIntGen(mv);

		mv.visitVarInsn(ALOAD, locals + VAR_MAP);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(mapType), "iterator", getMethodDescriptor(getType(Iterator.class)));
		mv.visitVarInsn(ASTORE, locals + VAR_I);

		Label loop = new Label();
		mv.visitLabel(loop);

		mv.visitVarInsn(ALOAD, locals + VAR_I);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(Iterator.class), "hasNext", getMethodDescriptor(BOOLEAN_TYPE));
		Label exit = new Label();
		mv.visitJumpInsn(IFEQ, exit);

		mv.visitVarInsn(ALOAD, locals + VAR_I);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(Iterator.class), "next", getMethodDescriptor(getType(Object.class)));
		mv.visitTypeInsn(CHECKCAST, getInternalName(iteratorType));
		mv.visitVarInsn(ASTORE, locals + VAR_CURSOR);

		mv.visitVarInsn(ALOAD, varContainer);
		mv.visitVarInsn(ALOAD, locals + VAR_CURSOR);
		mv.visitFieldInsn(GETFIELD, getInternalName(iteratorType), "key", getDescriptor(keyType));
		serializerCaller.serialize(keySerializer, version, mv, locals + VAR_LAST, varContainer, keyType);

		mv.visitVarInsn(ALOAD, varContainer);
		mv.visitVarInsn(ALOAD, locals + VAR_CURSOR);
		mv.visitFieldInsn(GETFIELD, getInternalName(iteratorType), "value", getDescriptor(valueType));
		serializerCaller.serialize(valueSerializer, version, mv, locals + VAR_LAST, varContainer, valueType);

		mv.visitJumpInsn(GOTO, loop);

		mv.visitLabel(exit);
	}

	@Override
	public void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals,
	                        SerializerCaller serializerCaller, Class<?> targetType) {
		mv.visitTypeInsn(NEW, getInternalName(hashMapType));
		mv.visitVarInsn(ASTORE, locals + VAR_MAP);
		mv.visitVarInsn(ALOAD, locals + VAR_MAP);
		mv.visitMethodInsn(INVOKESPECIAL, getInternalName(hashMapType), "<init>", getMethodDescriptor(VOID_TYPE));

		backend.readVarIntGen(mv);
		mv.visitVarInsn(ISTORE, locals + VAR_LENGTH);

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
		serializerCaller.deserialize(keySerializer, version, mv, locals + VAR_LAST, varContainer, keyType);
		mv.visitVarInsn(ALOAD, varContainer);
		serializerCaller.deserialize(valueSerializer, version, mv, locals + VAR_LAST, varContainer, valueType);

		mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(hashMapType), "put", getMethodDescriptor(getType(valueType), getType(keyType), getType(valueType)));

		if (getType(valueType).getSize() == 2)
			mv.visitInsn(POP2);
		else
			mv.visitInsn(POP);

		mv.visitIincInsn(locals + VAR_I, 1);
		mv.visitJumpInsn(GOTO, loop);
		mv.visitLabel(exit);

		mv.visitVarInsn(ALOAD, locals + VAR_MAP);
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
	public Class<?> getRawType() throws RuntimeException {
		return mapType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		SerializerGenHppcMap that = (SerializerGenHppcMap) o;

		return (keySerializer.equals(that.keySerializer)) && (valueSerializer.equals(that.valueSerializer));
	}

	@Override
	public int hashCode() {
		int result = keySerializer.hashCode();
		return 31 * result + valueSerializer.hashCode();
	}
}
