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
import static com.google.common.base.Preconditions.*;
import static com.google.common.base.Throwables.propagate;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;

import java.util.Iterator;

import com.google.common.collect.ImmutableMap;
import io.datakernel.serializer.SerializerCaller;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

public class SerializerGenHppcSet implements SerializerGen {
	private static final int VAR_SET = 0;
	private static final int VAR_LENGTH = 1;
	private static final int VAR_I = 2;
	private static final int VAR_CURSOR = 3;
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

	public static SerializerGenBuilder serializerGenBuilder(final Class<?> setType, final Class<?> valueType) {
		String prefix = LOWER_CAMEL.to(UPPER_CAMEL, valueType.getSimpleName());
		checkArgument(setType.getSimpleName().startsWith(prefix), "Expected setType '%s', but was begin '%s'", setType.getSimpleName(), prefix);
		return new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, final SerializerForType[] generics, SerializerGen fallback) {
				SerializerGen valueSerializer;
				if (generics.length == 1) {
					if (valueType == Object.class) {
						valueSerializer = generics[0].serializer;
					} else {
						throw new IllegalArgumentException("keyClass or valueType must be Object.class");
					}
				} else {
					valueSerializer = primitiveSerializers.get(valueType);
				}
				return new SerializerGenHppcSet(setType, valueType, checkNotNull(valueSerializer));
			}
		};
	}

	private final Class<?> setType;
	private final Class<?> hashSetType;
	private final Class<?> iteratorType;
	private final Class<?> valueType;
	private final SerializerGen valueSerializer;

	private SerializerGenHppcSet(Class<?> setType, Class<?> valueType, SerializerGen valueSerializer) {
		this.setType = setType;
		this.valueType = valueType;
		this.valueSerializer = valueSerializer;
		try {
			String prefix = LOWER_CAMEL.to(UPPER_CAMEL, valueType.getSimpleName());
			this.iteratorType = Class.forName("com.carrotsearch.hppc.cursors." + prefix + "Cursor");
			this.hashSetType = Class.forName("com.carrotsearch.hppc." + prefix + "OpenHashSet");
		} catch (ClassNotFoundException e) {
			throw propagate(e);
		}
	}

	@Override
	public void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller,
	                      Class<?> sourceType) {
		mv.visitVarInsn(ASTORE, locals + VAR_SET);
		mv.visitVarInsn(ALOAD, locals + VAR_SET);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(setType), "size", getMethodDescriptor(INT_TYPE));
		backend.writeVarIntGen(mv);

		mv.visitVarInsn(ALOAD, locals + VAR_SET);
		mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(setType), "iterator", getMethodDescriptor(getType(Iterator.class)));
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
		mv.visitFieldInsn(GETFIELD, getInternalName(iteratorType), "value", getDescriptor(valueType));
		serializerCaller.serialize(valueSerializer, version, mv, locals + VAR_LAST, varContainer, valueType);

		mv.visitJumpInsn(GOTO, loop);

		mv.visitLabel(exit);
	}

	@Override
	public void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals,
	                        SerializerCaller serializerCaller, Class<?> targetType) {
		mv.visitTypeInsn(NEW, getInternalName(hashSetType));
		mv.visitVarInsn(ASTORE, locals + VAR_SET);
		mv.visitVarInsn(ALOAD, locals + VAR_SET);
		mv.visitMethodInsn(INVOKESPECIAL, getInternalName(hashSetType), "<init>", getMethodDescriptor(VOID_TYPE));

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

		mv.visitVarInsn(ALOAD, locals + VAR_SET);

		mv.visitVarInsn(ALOAD, varContainer);
		serializerCaller.deserialize(valueSerializer, version, mv, locals + VAR_LAST, varContainer, valueType);
		mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(hashSetType), "add", getMethodDescriptor(BOOLEAN_TYPE, getType(valueType)));
		mv.visitInsn(POP);

		mv.visitIincInsn(locals + VAR_I, 1);
		mv.visitJumpInsn(GOTO, loop);
		mv.visitLabel(exit);

		mv.visitVarInsn(ALOAD, locals + VAR_SET);
	}

	@Override
	public void getVersions(VersionsCollector versions) {
		versions.addRecursive(valueSerializer);
	}

	@Override
	public boolean isInline() {
		return false;
	}

	@Override
	public Class<?> getRawType() {
		return setType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		SerializerGenHppcSet that = (SerializerGenHppcSet) o;

		return valueSerializer.equals(that.valueSerializer);
	}

	@Override
	public int hashCode() {
		return 31 * valueSerializer.hashCode();
	}
}
