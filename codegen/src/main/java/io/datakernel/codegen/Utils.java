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

import com.google.common.primitives.Primitives;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagate;
import static java.lang.ClassLoader.getSystemClassLoader;
import static org.objectweb.asm.Type.getType;

class Utils {

	private static final Map<String, Type> wrapperToPrimitive = new HashMap<>();

	static {
		for (Class<?> primitiveType : Primitives.allPrimitiveTypes()) {
			Class<?> wrappedType = Primitives.wrap(primitiveType);
			wrapperToPrimitive.put(wrappedType.getName(), getType(primitiveType));
		}
	}

	private static final Type BOOLEAN_TYPE = getType(Boolean.class);
	private static final Type CHARACTER_TYPE = getType(Character.class);
	private static final Type BYTE_TYPE = getType(Byte.class);
	private static final Type SHORT_TYPE = getType(Short.class);
	private static final Type INT_TYPE = getType(Integer.class);
	private static final Type FLOAT_TYPE = getType(Float.class);
	private static final Type LONG_TYPE = getType(Long.class);
	private static final Type DOUBLE_TYPE = getType(Double.class);
	private static final Type VOID_TYPE = getType(Void.class);

	public static boolean isPrimitiveType(Type type) {
		int sort = type.getSort();
		return sort == Type.BOOLEAN ||
				sort == Type.CHAR ||
				sort == Type.BYTE ||
				sort == Type.SHORT ||
				sort == Type.INT ||
				sort == Type.FLOAT ||
				sort == Type.LONG ||
				sort == Type.DOUBLE ||
				sort == Type.VOID;
	}

	public static boolean isWrapperType(Type type) {
		return type.getSort() == Type.OBJECT &&
				wrapperToPrimitive.containsKey(type.getClassName());
	}

	public static Type wrap(Type type) {
		int sort = type.getSort();
		if (sort == Type.BOOLEAN)
			return BOOLEAN_TYPE;
		if (sort == Type.CHAR)
			return CHARACTER_TYPE;
		if (sort == Type.BYTE)
			return BYTE_TYPE;
		if (sort == Type.SHORT)
			return SHORT_TYPE;
		if (sort == Type.INT)
			return INT_TYPE;
		if (sort == Type.FLOAT)
			return FLOAT_TYPE;
		if (sort == Type.LONG)
			return LONG_TYPE;
		if (sort == Type.DOUBLE)
			return DOUBLE_TYPE;
		if (sort == Type.VOID)
			return VOID_TYPE;
		throw new IllegalArgumentException();
	}

	public static Type unwrap(Type type) {
		checkArgument(type.getSort() == Type.OBJECT);
		Type result = wrapperToPrimitive.get(type.getClassName());
		checkArgument(result != null);
		return result;
	}

	public static Type complementTypeOrNull(Type type) {
		if (isPrimitiveType(type))
			return wrap(type);
		if (isWrapperType(type))
			return unwrap(type);
		return null;
	}

	public static Type complementType(Type type) {
		Type result = complementTypeOrNull(type);
		if (result != null)
			return result;
		throw new IllegalArgumentException();
	}

	public static Class<?> getJavaType(ClassLoader classLoader, Type type) {
		int sort = type.getSort();
		if (sort == Type.BOOLEAN)
			return boolean.class;
		if (sort == Type.CHAR)
			return char.class;
		if (sort == Type.BYTE)
			return byte.class;
		if (sort == Type.SHORT)
			return short.class;
		if (sort == Type.INT)
			return int.class;
		if (sort == Type.FLOAT)
			return float.class;
		if (sort == Type.LONG)
			return long.class;
		if (sort == Type.DOUBLE)
			return double.class;
		if (sort == Type.VOID)
			return void.class;
		if (sort == Type.OBJECT) {
			try {
				return classLoader.loadClass(type.getClassName());
			} catch (ClassNotFoundException e) {
				throw propagate(e);
			}
		}
		if (sort == Type.ARRAY) {
			throw new UnsupportedOperationException(); // TODO
		}
		throw new IllegalArgumentException();
	}

	public static Class<?> getJavaType(Type type) {
		return getJavaType(getSystemClassLoader(), type);
	}

	public static void invokeVirtualOrInterface(GeneratorAdapter g, Class<?> owner, Method method) {
		if (owner.isInterface())
			g.invokeInterface(getType(owner), method);
		else
			g.invokeVirtual(getType(owner), method);
	}

	public static FunctionDef thisVar() {
		return new VarThis();
	}

	public static VarLocal newLocal(Context ctx, Type type) {
		int local = ctx.getGeneratorAdapter().newLocal(type);
		return new VarLocal(local);
	}

	public static VarLocal newLocal(Context ctx, Type type, String name) {
		VarLocal varLocal = newLocal(ctx, type);
		ctx.putLocal(name, varLocal);
		return varLocal;
	}

	public static FunctionDef argumentVar(int argument) {
		return new VarArg(argument);
	}

	public static void loadAndCast(Context ctx, FunctionDef functionDef, Type targetType) {
		Type type = functionDef.load(ctx);
		cast(ctx, type, targetType);
	}

	public static void cast(Context ctx, Type type, Type targetType) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		if (type.equals(targetType)) {
			return;
		}

		if (targetType == Type.VOID_TYPE) {
			if (type.getSize() == 1)
				g.pop();
			if (type.getSize() == 2)
				g.pop2();
			return;
		}

		if (type == Type.VOID_TYPE) {
			throw new IllegalArgumentException();
		}

		if (type.equals(ctx.getThisType())) {
			if (getJavaType(ctx.getClassLoader(), targetType).isAssignableFrom(ctx.getThisSuperclass())) {
				return;
			}
			throw new IllegalArgumentException();
		}

		if (!type.equals(ctx.getThisType()) && !targetType.equals(ctx.getThisType()) &&
				getJavaType(ctx.getClassLoader(), targetType).isAssignableFrom(getJavaType(ctx.getClassLoader(), type))) {
			return;
		}

		if ((isPrimitiveType(type) || isWrapperType(type)) &&
				(isPrimitiveType(targetType) || isWrapperType(targetType))) {

			Type targetTypePrimitive = isPrimitiveType(targetType) ? targetType : unwrap(targetType);

			if (isWrapperType(type)) {
				type = unwrap(type);
				g.unbox(type);
			}

			assert isPrimitiveType(type);

			if (type != targetTypePrimitive) {
				g.cast(type, targetTypePrimitive);
			}

			if (isWrapperType(targetType)) {
				g.box(targetTypePrimitive);
			}

			return;
		}

		g.checkCast(targetType);
	}
}
