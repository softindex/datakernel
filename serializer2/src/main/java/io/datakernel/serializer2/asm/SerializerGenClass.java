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

package io.datakernel.serializer2.asm;

import io.datakernel.codegen.AsmFunctionFactory;
import io.datakernel.codegen.FunctionDef;
import io.datakernel.codegen.VarField;
import io.datakernel.serializer2.SerializerCodeGenFactory;
import io.datakernel.serializer2.SerializerStaticCaller;
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
import static org.objectweb.asm.Type.getType;

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
	public FunctionDef serialize(FunctionDef value, SerializerGen serializerGen, int version, SerializerStaticCaller serializerCaller) {
		List<FunctionDef> list = new ArrayList<>();
		for (String fieldName : fields.keySet()) {

			FieldGen fieldGen = fields.get(fieldName);
			if (!fieldGen.hasVersion(version)) continue;

			Class<?> type = fieldGen.serializer.getRawType();
			if (!fieldGen.getRawType().equals(Object.class)) type = fieldGen.getRawType();

			if (fieldGen.field != null) {
				list.add(serializerCaller.serialize(fieldGen.serializer, cast(field(value, fieldName), type), version));
			} else if (fieldGen.method != null) {
				list.add(serializerCaller.serialize(fieldGen.serializer, cast(call(value, fieldGen.method.getName()), type), version));
			} else throw new AssertionError();
		}

		return sequence(list);
	}

	@Override
	public FunctionDef deserialize(Class<?> targetType, final int version, SerializerStaticCaller serializerCaller) {
		if (!implInterface && dataTypeIn.isInterface()) {
			return deserializeInterface(targetType, version, serializerCaller);
		}
		if (!implInterface && constructor == null && factory == null && setters.isEmpty()) {
			return deserializeClassSimple(targetType, version, serializerCaller);
		}

		final Map<String, FunctionDef> map = new HashMap<>();
		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);

			if (!fieldGen.hasVersion(version)) continue;

			FunctionDef functionDef = serializerCaller.deserialize(fieldGen.serializer, version, fieldGen.getRawType());
			map.put(fieldName, cast(functionDef, fieldGen.getRawType()));
		}

		final Set<String> used = new HashSet<>();

		FunctionDef creater;
		if (factory == null) {
			creater = _insertCallConstructor(targetType, used, map);
		} else {
			creater = _insertCallFactory(targetType, map, used);
		}

		FunctionDef local = let(creater);
		List<FunctionDef> list = new ArrayList<>();
		list.add(local);

		if (!setters.isEmpty()) {
			List<String> listParam = new ArrayList<>();
			final Map<List<String>, Method> paramMethod = new HashMap<>();
			for (Method method : setters.keySet()) {
				paramMethod.put(setters.get(method), method);
			}

			for (String eachParam : fields.keySet()) {
				if (used.contains(eachParam)) continue;
				listParam.add(eachParam);
				if (paramMethod.containsKey(listParam)) {
					FunctionDef[] params = new FunctionDef[listParam.size()];
					for (int i = 0; i < listParam.size(); i++) {

						String paramName = listParam.get(i);
						params[i] = map.get(paramName);
					}
					list.add(call(local, paramMethod.get(listParam).getName(), params));
					for (String each : listParam) used.add(each);
					listParam.clear();
				}
			}
		}
		for (String fieldName : fields.keySet()) {
			if (used.contains(fieldName)) continue;
			FieldGen fieldGen = fields.get(fieldName);

			if (!fieldGen.hasVersion(version)) continue;

			VarField field = field(local, fieldName);
			list.add(set(field, map.get(fieldName)));
		}
		list.add(local);
		return sequence(list);

	}

	private FunctionDef _insertCallFactory(Class<?> targetType, Map<String, FunctionDef> map, Set<String> used) {
		FunctionDef[] param = new FunctionDef[factoryParams.size()];
		if (factory != null && factoryParams.size() != 0) {
			for (int i = 0; i < factoryParams.size(); i++) {
				param[i] = map.get(factoryParams.get(i));
				map.remove(factoryParams.get(i));
				used.add(factoryParams.get(i));
			}
		}
		return callFutureStatic(factory.getDeclaringClass().getName(), factory.getName(), getType(targetType), param);
	}

	private FunctionDef _insertCallConstructor(Class<?> targetType, final Set<String> used, final Map<String, FunctionDef> map) {
		FunctionDef[] param;
		if (constructorParams == null) {
			param = new FunctionDef[0];
		} else {
			param = new FunctionDef[constructorParams.size()];
		}

		if (constructorParams != null && !constructorParams.isEmpty()) {
			for (int i = 0; i < constructorParams.size(); i++) {
				param[i] = map.get(constructorParams.get(i));
				map.remove(constructorParams.get(i));
				used.add(constructorParams.get(i));
			}
		}
		return constructor(targetType, param);
	}

	private FunctionDef deserializeInterface(Class<?> targetType, final int version, final SerializerStaticCaller serializerCaller) {
		final AsmFunctionFactory<Object> asmFactory = new AsmFunctionFactory(SerializerCodeGenFactory.definingClassLoader, targetType);
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
			FunctionDef functionDef = serializerCaller.deserialize(fieldGen.serializer, version, fieldGen.getRawType());
			list.add(set(field, functionDef));
		}

		list.add(local);
		return sequence(list);
	}

	private FunctionDef deserializeClassSimple(final Class<?> targetType, final int version, final SerializerStaticCaller serializerCaller) {
		FunctionDef local = let(constructor(targetType));
		List<FunctionDef> list = new ArrayList<>();

		for (String fieldName : fields.keySet()) {
			FieldGen fieldGen = fields.get(fieldName);

			if (!fieldGen.hasVersion(version)) continue;

			VarField field = field(local, fieldName);
			FunctionDef functionDef = serializerCaller.deserialize(fieldGen.serializer, version, fieldGen.getRawType());
			list.add(set(field, functionDef));
		}
		list.add(local);
		return sequence(list);
	}
}
