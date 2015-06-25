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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.datakernel.serializer.SerializerCaller;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

import java.util.LinkedHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;

@SuppressWarnings("PointlessArithmeticExpression")
public class SerializerGenSubclass implements SerializerGen {
	private final static int VAR = 0;
	private final static int VAR_CLASS = 1;
	private final static int VAR_LAST = 2;

	private final static int VAR_N = 0;
	private final static int VAR_LAST2 = 1;

	public static final class Builder {
		private final Class<?> dataType;
		private final LinkedHashMap<Class<?>, SerializerGen> subclassSerializers = Maps.newLinkedHashMap();

		public Builder(Class<?> dataType) {
			this.dataType = dataType;
		}

		public Builder add(Class<?> subclass, SerializerGen serializer) {
			checkArgument(subclassSerializers.put(subclass, serializer) == null);
			return this;
		}

		public SerializerGenSubclass build() {
			return new SerializerGenSubclass(dataType, subclassSerializers);
		}
	}

	private final Class<?> dataType;
	private final ImmutableMap<Class<?>, SerializerGen> subclassSerializers;

	public SerializerGenSubclass(Class<?> dataType, LinkedHashMap<Class<?>, SerializerGen> subclassSerializers) {
		this.dataType = checkNotNull(dataType);
		this.subclassSerializers = ImmutableMap.copyOf(subclassSerializers);
	}

	@Override
	public void getVersions(VersionsCollector versions) {
		for (SerializerGen serializer : subclassSerializers.values()) {
			versions.addRecursive(serializer);
		}
	}

	@Override
	public boolean isInline() {
		return false;
	}

	@Override
	public Class<?> getRawType() {
		return dataType;
	}

	@Override
	public void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> sourceType) {
		mv.visitVarInsn(ASTORE, locals + VAR);
		mv.visitVarInsn(ALOAD, locals + VAR);
		mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(Object.class),
				"getClass", getMethodDescriptor(getType(Class.class)));
		mv.visitVarInsn(ASTORE, locals + VAR_CLASS);

		Label exit = new Label();

		int subclassN = 0;
		for (Class<?> subclass : subclassSerializers.keySet()) {
			SerializerGen subclassSerializer = subclassSerializers.get(subclass);
			mv.visitVarInsn(ALOAD, locals + VAR_CLASS);
			mv.visitLdcInsn(getType(subclass));
			Label next = new Label();
			mv.visitJumpInsn(IF_ACMPNE, next);
			mv.visitLdcInsn(subclassN++);
			backend.writeByteGen(mv);

			mv.visitVarInsn(ALOAD, varContainer);
			mv.visitVarInsn(ALOAD, locals + VAR);
			mv.visitTypeInsn(CHECKCAST, getInternalName(subclass));
			serializerCaller.serialize(subclassSerializer, version, mv, locals + VAR_LAST, varContainer, sourceType);

			mv.visitJumpInsn(GOTO, exit);
			mv.visitLabel(next);
		}

		mv.visitTypeInsn(NEW, getInternalName(IllegalArgumentException.class));
		mv.visitInsn(DUP);
		mv.visitMethodInsn(INVOKESPECIAL, getInternalName(IllegalArgumentException.class),
				"<init>", getMethodDescriptor(VOID_TYPE));
		mv.visitInsn(ATHROW);

		mv.visitLabel(exit);
	}

	@Override
	public void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> targetType) {
		backend.readByteGen(mv);
		mv.visitVarInsn(ISTORE, locals + VAR_N);

		Label exit = new Label();

		int subclassN = 0;
		for (Class<?> subclass : subclassSerializers.keySet()) {
			SerializerGen subclassSerializer = subclassSerializers.get(subclass);
			Label next = new Label();
			mv.visitVarInsn(ILOAD, locals + VAR_N);
			mv.visitLdcInsn(subclassN++);
			mv.visitJumpInsn(IF_ICMPNE, next);

			mv.visitVarInsn(ALOAD, varContainer);
			serializerCaller.deserialize(subclassSerializer, version, mv, locals + VAR_LAST2, varContainer, targetType);
			mv.visitJumpInsn(GOTO, exit);

			mv.visitLabel(next);
		}

		mv.visitTypeInsn(NEW, getInternalName(IllegalArgumentException.class));
		mv.visitInsn(DUP);
		mv.visitMethodInsn(INVOKESPECIAL, getInternalName(IllegalArgumentException.class),
				"<init>", getMethodDescriptor(VOID_TYPE));
		mv.visitInsn(ATHROW);

		mv.visitLabel(exit);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenSubclass that = (SerializerGenSubclass) o;

		return (dataType.equals(that.dataType)) && (subclassSerializers.equals(that.subclassSerializers));
	}

	@Override
	public int hashCode() {
		int result = dataType.hashCode();
		result = 31 * result + subclassSerializers.hashCode();
		return result;
	}
}
