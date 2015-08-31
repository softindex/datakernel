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

import io.datakernel.codegen.AsmFunctionFactory;
import io.datakernel.codegen.FunctionDef;
import io.datakernel.codegen.VarField;
import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer.SerializerFactory;
import org.objectweb.asm.Type;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import static io.datakernel.codegen.FunctionDefs.*;
import static io.datakernel.codegen.utils.Preconditions.check;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;
import static java.lang.reflect.Modifier.*;
import static java.util.Arrays.asList;
import static org.objectweb.asm.Type.*;

@SuppressWarnings("PointlessArithmeticExpression")
public class SerializerGenClass implements SerializerGen {

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

	private final Map<String, FieldGen> fields = new LinkedHashMap<>();
	private int lastOffset;

	private Constructor<?> constructor;
	private List<String> constructorParams;
	private Method factory;
	private List<String> factoryParams;
	private final Map<Method, List<String>> setters = new LinkedHashMap<>();

	public SerializerGenClass(Class<?> type) {
		this.dataTypeIn = checkNotNull(type);
		if (!dataTypeIn.isInterface()) {
			this.dataTypeOut = dataTypeIn;
		}
	}

	public SerializerGenClass(Class<?> type, Class<?> typeImpl) {
		checkNotNull(type);
		checkNotNull(typeImpl);
		check(type.isInterface());
		check(type.isAssignableFrom(typeImpl));
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
		check(implInterface || !dataTypeIn.isInterface());
		checkNotNull(method);
		checkNotNull(fields);
		check(!isPrivate(method.getModifiers()), "Setter cannot be private: %s", method);
		check(method.getGenericParameterTypes().length == fields.size());
		check(!setters.containsKey(method));
		setters.put(method, fields);
	}

	public void setFactory(Method methodFactory, List<String> fields) {
		check(implInterface || !dataTypeIn.isInterface());
		checkNotNull(methodFactory);
		checkNotNull(fields);
		check(this.factory == null, "Factory is already set: %s", this.factory);
		check(!isPrivate(methodFactory.getModifiers()), "Factory cannot be private: %s", methodFactory);
		check(isStatic(methodFactory.getModifiers()), "Factory must be static: %s", methodFactory);
		check(methodFactory.getGenericParameterTypes().length == fields.size());
		this.factory = methodFactory;
		this.factoryParams = fields;
	}

	public void setConstructor(Constructor<?> constructor, List<String> fields) {
		check(implInterface || !dataTypeIn.isInterface());
		checkNotNull(constructor);
		checkNotNull(fields);
		check(this.constructor == null, "Constructor is already set: %s", this.constructor);
		check(!isPrivate(constructor.getModifiers()), "Constructor cannot be private: %s", constructor);
		check(constructor.getGenericParameterTypes().length == fields.size());
		this.constructor = constructor;
		this.constructorParams = fields;
	}

	public void addField(Field field, SerializerGen serializer, int added, int removed) {
		check(implInterface || !dataTypeIn.isInterface());
		check(isPublic(field.getModifiers()));
		String fieldName = field.getName();
		check(!fields.containsKey(fieldName), "Duplicate field '%s'", field);
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
		check(method.getGenericParameterTypes().length == 0);
		check(isPublic(method.getModifiers()));
		String fieldName = stripGet(method.getName(), method.getReturnType());
		check(!fields.containsKey(fieldName), "Duplicate field '%s'", method);
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
		check(implInterface || !dataTypeIn.isInterface());
		Set<String> usedFields = new HashSet<>();
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
				throw new RuntimeException(e);
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

		if (!dataTypeIn.equals(that.dataTypeIn)) return false;
		if (!generics.equals(that.generics)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		if (generics == null)
			return super.hashCode();

		int result = dataTypeIn.hashCode();
		result = 31 * result + generics.hashCode();
		return result;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerFactory.StaticMethods staticMethods) {
		if (staticMethods.startSerializeStaticMethod(this, version)) {
			return;
		}

		List<FunctionDef> list = new ArrayList<>();
		for (String fieldName : fields.keySet()) {

			FieldGen fieldGen = fields.get(fieldName);
			if (!fieldGen.hasVersion(version)) continue;

			Class<?> type = fieldGen.serializer.getRawType();
			if (!fieldGen.getRawType().equals(Object.class)) type = fieldGen.getRawType();

			if (fieldGen.field != null) {
				fieldGen.serializer.prepareSerializeStaticMethods(version, staticMethods);
				list.add(fieldGen.serializer.serialize(cast(field(arg(1), fieldName), type), version, staticMethods));
			} else if (fieldGen.method != null) {
				fieldGen.serializer.prepareSerializeStaticMethods(version, staticMethods);
				list.add(fieldGen.serializer.serialize(cast(call(arg(1), fieldGen.method.getName()), type), version, staticMethods));
			} else throw new AssertionError();
		}

		staticMethods.registerStaticSerializeMethod(this, version, sequence(list));
	}

	@Override
	public FunctionDef serialize(FunctionDef field, int version, SerializerFactory.StaticMethods staticMethods) {
		return staticMethods.callStaticSerializeMethod(this, version, arg(0), field);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerFactory.StaticMethods staticMethods) {
		if (staticMethods.startDeserializeStaticMethod(this, version)) {
			return;
		}

		if (!implInterface && dataTypeIn.isInterface()) {
			FunctionDef functionDef = deserializeInterface(this.getRawType(), version, staticMethods);
			staticMethods.registerStaticDeserializeMethod(this, version, functionDef);
			return;
		}
		if (!implInterface && constructor == null && factory == null && setters.isEmpty()) {
			FunctionDef functionDef = deserializeClassSimple(version, staticMethods);
			staticMethods.registerStaticDeserializeMethod(this, version, functionDef);
			return;
		}

		List<FunctionDef> list = new ArrayList<>();
		final Map<String, FunctionDef> map = new HashMap<>();
		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);

			if (!fieldGen.hasVersion(version)) continue;

			fieldGen.serializer.prepareDeserializeStaticMethods(version, staticMethods);
			FunctionDef functionDef = let(fieldGen.serializer.deserialize(fieldGen.getRawType(), version, staticMethods));
			list.add(functionDef);
			map.put(fieldName, cast(functionDef, fieldGen.getRawType()));
		}

		FunctionDef constructor;
		if (factory == null) {
			constructor = _insertCallConstructor(this.getRawType(), map);
		} else {
			constructor = _insertCallFactory(map);
		}

		FunctionDef local = let(constructor);
		list.add(local);

		for (Method method : setters.keySet()) {
			boolean found = false;
			for (String fieldName : setters.get(method)) {
				FieldGen fieldGen = fields.get(fieldName);
				Preconditions.checkNotNull(fieldGen, "Field '%s' is not found in '%s'", fieldName, method);
				if (fieldGen.hasVersion(version)) {
					found = true;
					break;
				}
			}
			if (found) {
				FunctionDef[] temp = new FunctionDef[method.getParameterTypes().length];
				int i = 0;
				for (String fieldName : setters.get(method)) {
					FieldGen fieldGen = fields.get(fieldName);
					assert fieldGen != null;
					if (fieldGen.hasVersion(version)) {
						temp[i++] = map.get(fieldName);
					} else {
						temp[i++] = pushDefaultValue(fieldGen.getAsmType());
					}
				}
				list.add(call(local, method.getName(), temp));
			}
		}

		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);
			if (!fieldGen.hasVersion(version))
				continue;
			if (fieldGen.field == null || isFinal(fieldGen.field.getModifiers()))
				continue;
			VarField field = field(local, fieldName);
			list.add(set(field, map.get(fieldName)));
		}

		list.add(local);
		staticMethods.registerStaticDeserializeMethod(this, version, sequence(list));
	}

	@Override
	public FunctionDef deserialize(Class<?> targetType, int version, SerializerFactory.StaticMethods staticMethods) {
		return staticMethods.callStaticDeserializeMethod(this, version, arg(0));
	}

	private FunctionDef _insertCallFactory(Map<String, FunctionDef> map) {
		FunctionDef[] param = new FunctionDef[factoryParams.size()];
		int i = 0;
		for (String fieldName : factoryParams) {
			param[i++] = map.get(fieldName);
		}
		return callStatic(factory.getDeclaringClass(), factory.getName(), param);
	}

	private FunctionDef _insertCallConstructor(Class<?> targetType, final Map<String, FunctionDef> map) {
		FunctionDef[] param;
		if (constructorParams == null) {
			param = new FunctionDef[0];
			return constructor(targetType, param);
		}
		param = new FunctionDef[constructorParams.size()];

		int i = 0;
		for (String fieldName : constructorParams) {
			param[i++] = map.get(fieldName);
		}
		return constructor(targetType, param);
	}

	private FunctionDef deserializeInterface(Class<?> targetType, final int version, SerializerFactory.StaticMethods staticMethods) {
		final AsmFunctionFactory<Object> asmFactory = new AsmFunctionFactory(staticMethods.getDefiningClassLoader(), targetType);
		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);

			Method method = checkNotNull(fieldGen.method);

			asmFactory.field(fieldName, method.getReturnType());
			asmFactory.method(method.getName(), field(self(), fieldName));
		}

		Class<?> newClass = asmFactory.defineClass();

		FunctionDef local = let(constructor(newClass));
		List<FunctionDef> list = new ArrayList<>();
		list.add(local);

		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);
			if (!fieldGen.hasVersion(version))
				continue;
			VarField field = field(local, fieldName);

			fieldGen.serializer.prepareDeserializeStaticMethods(version, staticMethods);
			FunctionDef functionDef = fieldGen.serializer.deserialize(fieldGen.getRawType(), version, staticMethods);
			list.add(set(field, functionDef));
		}

		list.add(local);
		return sequence(list);
	}

	private FunctionDef deserializeClassSimple(final int version, SerializerFactory.StaticMethods staticMethods) {
		FunctionDef local = let(constructor(this.getRawType()));

		List<FunctionDef> list = new ArrayList<>();
		list.add(local);
		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);

			if (!fieldGen.hasVersion(version)) continue;

			VarField field = field(local, fieldName);
			fieldGen.serializer.prepareDeserializeStaticMethods(version, staticMethods);
			list.add(set(field, fieldGen.serializer.deserialize(fieldGen.getRawType(), version, staticMethods)));
		}
		list.add(local);
		return sequence(list);
	}

	private FunctionDef pushDefaultValue(Type type) {
		switch (type.getSort()) {
			case BOOLEAN:
			case CHAR:
			case BYTE:
			case SHORT:
			case INT:
				return value(0);
			case Type.LONG:
				return value(0L);
			case Type.FLOAT:
				return value(0f);
			case Type.DOUBLE:
				return value(0d);
			case ARRAY:
			case OBJECT:
				return nullRef(type);
			default:
				throw new IllegalArgumentException();
		}
	}
}
