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

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.datakernel.asm.DefiningClassLoader;
import io.datakernel.asm.DefiningClassWriter;
import io.datakernel.serializer.SerializerCaller;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.*;
import static io.datakernel.serializer.asm.Utils.castSourceType;
import static java.lang.reflect.Modifier.*;
import static java.util.Arrays.asList;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;

@SuppressWarnings("PointlessArithmeticExpression")
public class SerializerGenClass implements SerializerGen {
	public static final String ASM_DATA_CLASS = "asm.DataClass";

	private static final AtomicInteger COUNTER = new AtomicInteger();

	public static final int VAR_ITEM = 0;
	public static final int VAR_LAST = VAR_ITEM + 1;

	private static final class FieldGen {
		private Field field;
		private Method method;
		private int offset;
		private int versionAdded = -1;
		private int versionDeleted = -1;
		private SerializerGen serializer;

		public boolean hasVersion(int version) {
			if (versionAdded == -1 && versionDeleted == -1) {
				return true;
			} else if (versionAdded != -1 && versionDeleted == -1) {
				return version >= versionAdded;
			} else if (versionAdded == -1) {
				return version < versionDeleted;
			} else {
				if (versionAdded > versionDeleted) {
					return version < versionDeleted || version >= versionAdded;
				} else if (versionAdded < versionDeleted) {
					return version >= versionAdded && version < versionDeleted;
				} else {
					throw new IllegalArgumentException();
				}
			}
		}

		public Class<?> getRawType() {
			if (field != null)
				return field.getType();
			if (method != null)
				return method.getReturnType();
			throw new AssertionError();
		}

		public Type getAsmType() {
			return getType(getRawType());
		}
	}

	private boolean implInterface;
	private Class<?> dataTypeIn;
	private Class<?> dataTypeOut;
	private List<SerializerGenBuilder.SerializerForType> generics;

	private final Map<String, FieldGen> fields = Maps.newLinkedHashMap();
	private int lastOffset;

	private Constructor<?> constructor;
	private List<String> constructorParams;
	private Method factory;
	private List<String> factoryParams;
	private final Map<Method, List<String>> setters = Maps.newLinkedHashMap();

	public SerializerGenClass(Class<?> type) {
		this.dataTypeIn = checkNotNull(type);
		if (!dataTypeIn.isInterface()) {
			this.dataTypeOut = dataTypeIn;
		}
	}

	public SerializerGenClass(Class<?> type, Class<?> typeImpl) {
		checkNotNull(type);
		checkNotNull(typeImpl);
		checkState(type.isInterface());
		checkArgument(type.isAssignableFrom(typeImpl));
		this.dataTypeIn = type;
		this.dataTypeOut = typeImpl;
		this.implInterface = true;
	}

	public SerializerGenClass(Class<?> type, SerializerGenBuilder.SerializerForType[] generics) {
		this(type);
		this.generics = asList(generics);
	}

	public SerializerGenClass(Class<?> type, SerializerGenBuilder.SerializerForType[] generics, Class<?> typeImpl) {
		this(type, typeImpl);
		this.generics = asList(generics);
	}

	public void addSetter(Method method, List<String> fields) {
		checkState(implInterface || !dataTypeIn.isInterface());
		checkNotNull(method);
		checkNotNull(fields);
		checkArgument(!isPrivate(method.getModifiers()), "Setter cannot be private: %s", method);
		checkArgument(method.getGenericParameterTypes().length == fields.size());
		checkState(!setters.containsKey(method));
		setters.put(method, fields);
	}

	public void setFactory(Method methodFactory, List<String> fields) {
		checkState(implInterface || !dataTypeIn.isInterface());
		checkNotNull(methodFactory);
		checkNotNull(fields);
		checkState(this.factory == null, "Factory is already set: %s", this.factory);
		checkArgument(!isPrivate(methodFactory.getModifiers()), "Factory cannot be private: %s", methodFactory);
		checkArgument(isStatic(methodFactory.getModifiers()), "Factory must be static: %s", methodFactory);
		checkArgument(methodFactory.getGenericParameterTypes().length == fields.size());
		this.factory = methodFactory;
		this.factoryParams = fields;
	}

	public void setConstructor(Constructor<?> constructor, List<String> fields) {
		checkState(implInterface || !dataTypeIn.isInterface());
		checkNotNull(constructor);
		checkNotNull(fields);
		checkState(this.constructor == null, "Constructor is already set: %s", this.constructor);
		checkArgument(!isPrivate(constructor.getModifiers()), "Constructor cannot be private: %s", constructor);
		checkArgument(constructor.getGenericParameterTypes().length == fields.size());
		this.constructor = constructor;
		this.constructorParams = fields;
	}

	public void addField(Field field, SerializerGen serializer, int added, int removed) {
		checkState(implInterface || !dataTypeIn.isInterface());
		checkArgument(isPublic(field.getModifiers()));
		String fieldName = field.getName();
		checkState(!fields.containsKey(fieldName), "Duplicate field '%s'", field);
		FieldGen fieldGen = new FieldGen();
		fieldGen.field = field;
		fieldGen.serializer = serializer;
		fieldGen.versionAdded = added;
		fieldGen.versionDeleted = removed;
		fieldGen.offset = lastOffset;
		lastOffset += getType(field.getType()).getSize();
		fields.put(fieldName, fieldGen);
	}

	public void addGetter(Method method, SerializerGen serializer, int added, int removed) {
		checkArgument(method.getGenericParameterTypes().length == 0);
		checkArgument(isPublic(method.getModifiers()));
		String fieldName = stripGet(method.getName(), method.getReturnType());
		checkState(!fields.containsKey(fieldName), "Duplicate field '%s'", method);
		FieldGen fieldGen = new FieldGen();
		fieldGen.method = method;
		fieldGen.serializer = serializer;
		fieldGen.versionAdded = added;
		fieldGen.versionDeleted = removed;
		fieldGen.offset = lastOffset;
		lastOffset += getType(method.getReturnType()).getSize();
		fields.put(fieldName, fieldGen);
	}

	public void addMatchingSetters() {
		checkState(implInterface || !dataTypeIn.isInterface());
		Set<String> usedFields = Sets.newHashSet();
		if (constructorParams != null) {
			usedFields.addAll(constructorParams);
		}
		if (factoryParams != null) {
			usedFields.addAll(factoryParams);
		}
		for (List<String> list : setters.values()) {
			usedFields.addAll(list);
		}
		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);
			Method getter = fieldGen.method;
			if (getter == null)
				continue;
			if (usedFields.contains(fieldName))
				continue;
			String setterName = "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
			try {
				Method setter;
				if (implInterface)
					setter = dataTypeOut.getMethod(setterName, getter.getReturnType());
				else
					setter = dataTypeIn.getMethod(setterName, getter.getReturnType());
				if (!isPrivate(setter.getModifiers())) {
					addSetter(setter, asList(fieldName));
				}
			} catch (NoSuchMethodException e) {
				throw Throwables.propagate(e);
			}
		}
	}

	@Override
	public void getVersions(VersionsCollector versions) {
		for (FieldGen fieldGen : fields.values()) {
			if (fieldGen.versionAdded != -1)
				versions.add(fieldGen.versionAdded);
			if (fieldGen.versionDeleted != -1)
				versions.add(fieldGen.versionDeleted);
			versions.addRecursive(fieldGen.serializer);
		}
	}

	@Override
	public boolean isInline() {
		return false;
	}

	@Override
	public Class<?> getRawType() {
		return dataTypeIn;
	}

	private void defineInterfaceImplementation(DefiningClassLoader classLoader) {
		checkState(setters.isEmpty());
		checkState(constructor == null);

		if (dataTypeOut != null) {
			return;
		}
		DefiningClassWriter cw = new DefiningClassWriter(classLoader);

		String className = ASM_DATA_CLASS + COUNTER.incrementAndGet();
		Type classType = getType('L' + className.replace('.', '/') + ';');

		cw.visit(V1_7, ACC_PUBLIC + ACC_FINAL + ACC_SUPER,
				classType.getInternalName(),
				null, getInternalName(Object.class),
				new String[]{getInternalName(dataTypeIn)});

		{
			MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
			mv.visitCode();
			mv.visitVarInsn(ALOAD, 0);
			mv.visitMethodInsn(INVOKESPECIAL, getInternalName(Object.class), "<init>", "()V");
			mv.visitInsn(RETURN);
			mv.visitMaxs(1, 1);
			mv.visitEnd();
		}

		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);
			Method method = checkNotNull(fieldGen.method);
			Type asmType = fieldGen.getAsmType();

			FieldVisitor fv = cw.visitField(ACC_PUBLIC, fieldName, asmType.getDescriptor(), null, null);
			fv.visitEnd();

			MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, method.getName(), "()" + asmType.getDescriptor(), null, null);
			mv.visitCode();
			mv.visitVarInsn(ALOAD, 0);
			mv.visitFieldInsn(GETFIELD, classType.getInternalName(), fieldName, asmType.getDescriptor());
			mv.visitInsn(asmType.getOpcode(IRETURN));
			mv.visitMaxs(1, 1);
			mv.visitEnd();
		}
		cw.visitEnd();

		dataTypeOut = classLoader.defineClass(className, cw.toByteArray());
	}

	@Override
	public void serialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> sourceType) {
		castSourceType(mv, sourceType, dataTypeIn);

		mv.visitVarInsn(ASTORE, locals + VAR_ITEM);
		mv.visitInsn(POP);
		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);
			if (!fieldGen.hasVersion(version))
				continue;
			Type asmType = fieldGen.getAsmType();
			mv.visitVarInsn(ALOAD, varContainer);
			mv.visitVarInsn(ALOAD, locals + VAR_ITEM);
			if (fieldGen.field != null)
				mv.visitFieldInsn(GETFIELD,
						getInternalName(dataTypeIn), fieldGen.field.getName(), asmType.getDescriptor());
			else if (fieldGen.method != null)
				mv.visitMethodInsn(dataTypeIn.isInterface() ? INVOKEINTERFACE : INVOKEVIRTUAL,
						getInternalName(dataTypeIn), fieldGen.method.getName(), "()" + asmType.getDescriptor());
			else throw new AssertionError();
			serializerCaller.serialize(fieldGen.serializer, version, mv, locals + VAR_LAST, varContainer, fieldGen.getRawType());
		}
	}

	private void deserializeInterface(int version, MethodVisitor mv, int varContainer, int locals, SerializerCaller serializerCaller) {
		checkState(constructor == null);
		checkState(factory == null);
		checkState(setters.isEmpty());

		mv.visitInsn(POP);
		mv.visitTypeInsn(NEW, getInternalName(dataTypeOut));
		mv.visitInsn(DUP);
		mv.visitMethodInsn(INVOKESPECIAL, getInternalName(dataTypeOut), "<init>", "()V");
		mv.visitVarInsn(ASTORE, locals + VAR_ITEM);

		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);
			if (!fieldGen.hasVersion(version))
				continue;
			mv.visitVarInsn(ALOAD, locals + VAR_ITEM);
			mv.visitVarInsn(ALOAD, varContainer);
			serializerCaller.deserialize(fieldGen.serializer, version, mv, locals + VAR_LAST, varContainer, fieldGen.getRawType());
			mv.visitFieldInsn(PUTFIELD,
					getInternalName(dataTypeOut), fieldName, fieldGen.getAsmType().getDescriptor());
		}

		mv.visitVarInsn(ALOAD, locals + VAR_ITEM);
	}

	private void deserializeClassSimple(int version, MethodVisitor mv, int varContainer, int locals, SerializerCaller serializerCaller) {
		checkState(constructor == null && factory == null);
		checkState(setters.isEmpty());

		mv.visitInsn(POP);
		mv.visitTypeInsn(NEW, getInternalName(dataTypeOut));
		mv.visitInsn(DUP);
		mv.visitMethodInsn(INVOKESPECIAL, getInternalName(dataTypeOut), "<init>", "()V");
		mv.visitVarInsn(ASTORE, locals + VAR_ITEM);

		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);
			if (!fieldGen.hasVersion(version))
				continue;
			mv.visitVarInsn(ALOAD, locals + VAR_ITEM);
			mv.visitVarInsn(ALOAD, varContainer);
			serializerCaller.deserialize(fieldGen.serializer, version, mv, locals + VAR_LAST, varContainer, fieldGen.getRawType());
			mv.visitFieldInsn(PUTFIELD,
					getInternalName(dataTypeOut), fieldGen.field.getName(), fieldGen.getAsmType().getDescriptor());
		}

		mv.visitVarInsn(ALOAD, locals + VAR_ITEM);
	}

	@Override
	public void deserialize(int version, MethodVisitor mv, SerializerBackend backend, int varContainer, int locals, SerializerCaller serializerCaller, Class<?> targetType) {
		if (!implInterface && dataTypeIn.isInterface()) {
			defineInterfaceImplementation(serializerCaller.getClassLoader());
			deserializeInterface(version, mv, varContainer, locals, serializerCaller);
			return;
		}

		if (!implInterface && constructor == null && factory == null && setters.isEmpty()) {
			deserializeClassSimple(version, mv, varContainer, locals, serializerCaller);
			return;
		}

		mv.visitInsn(POP);

		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);
			if (!fieldGen.hasVersion(version))
				continue;
			mv.visitVarInsn(ALOAD, varContainer);
			serializerCaller.deserialize(fieldGen.serializer, version, mv, locals + VAR_LAST + lastOffset, varContainer, fieldGen.getRawType());
			mv.visitVarInsn(fieldGen.getAsmType().getOpcode(ISTORE), locals + VAR_LAST + fieldGen.offset);
		}

		if (factory != null) {
			insertCallStaticFactory(mv, locals, version);
		} else {
			insertCallConstructor(mv, locals, version);
		}

		for (Method method : setters.keySet()) {
			boolean found = false;
			for (String fieldName : setters.get(method)) {
				FieldGen fieldGen = fields.get(fieldName);
				checkNotNull(fieldGen, "Field '%s' is not found in '%s'", fieldName, method);
				if (fieldGen.hasVersion(version)) {
					found = true;
					break;
				}
			}
			if (found) {
				mv.visitVarInsn(ALOAD, locals + VAR_ITEM);
				for (String fieldName : setters.get(method)) {
					FieldGen fieldGen = fields.get(fieldName);
					assert fieldGen != null;
					if (fieldGen.hasVersion(version)) {
						mv.visitVarInsn(fieldGen.getAsmType().getOpcode(ILOAD), locals + VAR_LAST + fieldGen.offset);
					} else {
						pushDefaultValue(mv, fieldGen.getAsmType());
					}
				}
				mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(dataTypeOut), method.getName(), getMethodDescriptor(method));
				if (getType(method.getReturnType()).getSize() == 1)
					mv.visitInsn(POP);
				if (getType(method.getReturnType()).getSize() == 2)
					mv.visitInsn(POP2);
			}
		}

		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);
			if (!fieldGen.hasVersion(version))
				continue;
			if (fieldGen.field == null || isFinal(fieldGen.field.getModifiers()))
				continue;
			mv.visitVarInsn(ALOAD, locals + VAR_ITEM);
			mv.visitVarInsn(fieldGen.getAsmType().getOpcode(ILOAD), locals + VAR_LAST + fieldGen.offset);
			mv.visitFieldInsn(PUTFIELD,
					getInternalName(dataTypeOut), fieldGen.field.getName(), fieldGen.getAsmType().getDescriptor());
		}

		mv.visitVarInsn(ALOAD, locals + VAR_ITEM);
	}

	private void insertCallConstructor(MethodVisitor mv, int locals, int version) {
		mv.visitTypeInsn(NEW, getInternalName(dataTypeOut));
		mv.visitInsn(DUP);
		if (constructor != null) {
			for (String fieldName : constructorParams) {
				FieldGen fieldGen = fields.get(fieldName);
				checkNotNull(fieldGen, "Field '%s' is not found in '%s'", fieldName, constructor);
				if (fieldGen.hasVersion(version)) {
					mv.visitVarInsn(fieldGen.getAsmType().getOpcode(ILOAD), locals + VAR_LAST + fieldGen.offset);
				} else {
					pushDefaultValue(mv, fieldGen.getAsmType());
				}
			}
		}
		mv.visitMethodInsn(INVOKESPECIAL,
				getInternalName(dataTypeOut),
				"<init>",
				constructor != null ? getConstructorDescriptor(constructor) : "()V");
		mv.visitVarInsn(ASTORE, locals + VAR_ITEM);
	}

	private void insertCallStaticFactory(MethodVisitor mv, int locals, int version) {
		for (String fieldName : factoryParams) {
			FieldGen fieldGen = fields.get(fieldName);
			checkNotNull(fieldGen, "Field '%s' is not found in '%s'", fieldName, factory);
			if (fieldGen.hasVersion(version)) {
				mv.visitVarInsn(fieldGen.getAsmType().getOpcode(ILOAD), locals + VAR_LAST + fieldGen.offset);
			} else {
				pushDefaultValue(mv, fieldGen.getAsmType());
			}
		}
		mv.visitMethodInsn(INVOKESTATIC,
				getInternalName(factory.getDeclaringClass()),
				factory.getName(),
				getMethodDescriptor(factory));
		mv.visitVarInsn(ASTORE, locals + VAR_ITEM);
	}

	private static void pushDefaultValue(MethodVisitor mv, Type type) {
		switch (type.getSort()) {
			case BOOLEAN:
			case CHAR:
			case BYTE:
			case SHORT:
			case INT:
				mv.visitInsn(ICONST_0);
				break;
			case Type.LONG:
				mv.visitInsn(LCONST_0);
				break;
			case Type.FLOAT:
				mv.visitInsn(FCONST_0);
				break;
			case Type.DOUBLE:
				mv.visitInsn(DCONST_0);
				break;
			case ARRAY:
			case OBJECT:
				mv.visitInsn(ACONST_NULL);
				break;
			default:
				throw new IllegalArgumentException();
		}
	}

	private static String stripGet(String getterName, Class<?> type) {
		if (type == Boolean.TYPE || type == Boolean.class) {
			if (getterName.startsWith("is") && getterName.length() > 2) {
				return Character.toLowerCase(getterName.charAt(2)) + getterName.substring(3);
			}
		}
		if (getterName.startsWith("get") && getterName.length() > 3) {
			return Character.toLowerCase(getterName.charAt(3)) + getterName.substring(4);
		}
		return getterName;
	}

	@Override
	public boolean equals(Object o) {
		if (generics == null)
			return super.equals(o);

		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenClass that = (SerializerGenClass) o;

		return (dataTypeIn.equals(that.dataTypeIn)) && (generics.equals(that.generics));
	}

	@Override
	public int hashCode() {
		if (generics == null)
			return super.hashCode();

		int result = dataTypeIn.hashCode();
		result = 31 * result + generics.hashCode();
		return result;
	}
}
