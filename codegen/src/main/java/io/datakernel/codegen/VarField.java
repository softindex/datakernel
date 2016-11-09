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

package io.datakernel.codegen;

import io.datakernel.codegen.utils.Primitives;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static io.datakernel.codegen.Utils.*;
import static java.lang.Character.toUpperCase;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 * Defines methods which allow to take field according to the name
 */
public final class VarField implements Variable {
	private final Expression owner;
	private final String field;

	VarField(Expression owner, String field) {
		this.owner = owner;
		this.field = field;
	}

	@Override
	public Type type(Context ctx) {
		return typeOfFieldOrGetter(ctx, owner.type(ctx), field);
	}

	@Override
	public Type load(Context ctx) {
		Type ownerType = owner.load(ctx);
		return loadFieldOrGetter(ctx, ownerType, field);
	}

	@Override
	public Object beginStore(Context ctx) {
		return owner.load(ctx);
	}

	@Override
	public void store(Context ctx, Object storeContext, Type type) {
		setField(ctx, (Type) storeContext, field, type);
	}

	public static void setField(Context ctx, Type ownerType, String field, Type valueType) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Class<?> valueClass = getJavaType(ctx.getClassLoader(), valueType);

		if (ctx.getThisType().equals(ownerType)) {
			Class<?> fieldClass = ctx.getFields().get(field);
			if (fieldClass == null) {
				throw new RuntimeException(format("No field \"%s\" in generated class %s. %s",
						field,
						ctx.getThisType().getClassName(),
						exceptionInGeneratedClass(ctx)));
			}
			Type fieldType = getType(fieldClass);
			cast(ctx, valueType, fieldType);
			g.putField(ownerType, field, fieldType);
			return;
		}

		Class<?> argumentClass = getJavaType(ctx.getClassLoader(), ownerType);

		try {
			java.lang.reflect.Field javaField = argumentClass.getField(field);
			if (Modifier.isPublic(javaField.getModifiers())) {
				Type fieldType = getType(javaField.getType());
				cast(ctx, valueType, fieldType);
				g.putField(ownerType, field, fieldType);
				return;
			}
		} catch (NoSuchFieldException ignored) {
		}

		java.lang.reflect.Method javaSetter = tryFindSetter(argumentClass, field, valueClass);

		if (javaSetter == null && Primitives.isWrapperType(valueClass)) {
			javaSetter = tryFindSetter(argumentClass, field, Primitives.unwrap(valueClass));
		}

		if (javaSetter == null && valueClass.isPrimitive()) {
			javaSetter = tryFindSetter(argumentClass, field, Primitives.wrap(valueClass));
		}

		if (javaSetter == null) {
			javaSetter = tryFindSetter(argumentClass, field);
		}

		if (javaSetter != null) {
			Type fieldType = getType(javaSetter.getParameterTypes()[0]);
			cast(ctx, valueType, fieldType);
			invokeVirtualOrInterface(g, argumentClass, getMethod(javaSetter));
			Type returnType = getType(javaSetter.getReturnType());
			if (returnType.getSize() == 1) {
				g.pop();
			} else if (returnType.getSize() == 2) {
				g.pop2();
			}
			return;
		}

		throw new RuntimeException(format("No public field or setter for class %s for field \"%s\". %s ",
				ownerType.getClassName(),
				field,
				exceptionInGeneratedClass(ctx))
		);
	}

	private static Method tryFindSetter(Class<?> argumentClass, String field, Class<?> valueClass) {
		Method m = null;
		try {
			m = argumentClass.getDeclaredMethod(field, valueClass);
		} catch (NoSuchMethodException ignored) {
		}

		if (m == null && field.length() >= 1) {
			try {
				m = argumentClass.getDeclaredMethod("set" + toUpperCase(field.charAt(0)) + field.substring(1), valueClass);
			} catch (NoSuchMethodException ignored) {
			}
		}
		return m;
	}

	private static Method tryFindSetter(Class<?> argumentClass, String field) {
		String setterName = "set" + toUpperCase(field.charAt(0)) + field.substring(1);

		for (Method method : argumentClass.getDeclaredMethods()) {
			if (method.getParameterTypes().length != 1)
				continue;
			if (method.getName().equals(field) || method.getName().equals(setterName))
				return method;
		}
		return null;
	}

	public static Type loadFieldOrGetter(Context ctx, Type ownerType, String field) {
		return loadFieldOrGetter(ctx, ownerType, field, true);
	}

	public static Type typeOfFieldOrGetter(Context ctx, Type ownerType, String field) {
		return loadFieldOrGetter(ctx, ownerType, field, false);
	}

	private static Type loadFieldOrGetter(Context ctx, Type ownerType, String field, boolean load) {
		GeneratorAdapter g = load ? ctx.getGeneratorAdapter() : null;

		if (ownerType.equals(ctx.getThisType())) {
			Class<?> thisFieldClass = ctx.getFields().get(field);
			if (thisFieldClass != null) {
				Type resultType = Type.getType(thisFieldClass);
				if (g != null) {
					g.getField(ownerType, field, resultType);
				}
				return resultType;
			} else {
				throw new RuntimeException(format("No public field or getter for class %s for field \"%s\". %s",
						ownerType.getClassName(),
						field,
						exceptionInGeneratedClass(ctx)));
			}
		}

		Class<?> argumentClass = getJavaType(ctx.getClassLoader(), ownerType);

		try {
			Field javaField = argumentClass.getField(field);
			if (isPublic(javaField.getModifiers()) && !isStatic(javaField.getModifiers())) {
				Type resultType = Type.getType(javaField.getType());
				if (g != null) {
					g.getField(ownerType, field, resultType);
				}
				return resultType;
			}
		} catch (NoSuchFieldException ignored) {
		}

		java.lang.reflect.Method m = null;
		try {
			m = argumentClass.getDeclaredMethod(field);
		} catch (NoSuchMethodException ignored) {
		}

		if (m == null && field.length() >= 1) {
			try {
				m = argumentClass.getDeclaredMethod("get" + toUpperCase(field.charAt(0)) + field.substring(1));
			} catch (NoSuchMethodException ignored) {
			}
		}

		if (m != null) {
			Type resultType = getType(m.getReturnType());
			if (g != null) {
				invokeVirtualOrInterface(g, argumentClass, getMethod(m));
			}
			return resultType;
		}

		throw new RuntimeException(format("No public field or getter for class %s for field \"%s\". %s",
				ownerType.getClassName(),
				field,
				exceptionInGeneratedClass(ctx)));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		VarField that = (VarField) o;

		if (owner != null ? !owner.equals(that.owner) : that.owner != null) return false;
		if (field != null ? !field.equals(that.field) : that.field != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = owner != null ? owner.hashCode() : 0;
		result = 31 * result + (field != null ? field.hashCode() : 0);
		return result;
	}
}
