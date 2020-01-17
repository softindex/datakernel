package io.datakernel.specializer;

import org.objectweb.asm.Type;

import java.lang.reflect.Array;

import static java.lang.System.identityHashCode;

class Utils {

	static Class<?> getBoxedType(Class<?> type) {
		if (byte.class == type) return Byte.class;
		if (boolean.class == type) return Boolean.class;
		if (short.class == type) return Short.class;
		if (char.class == type) return Character.class;
		if (int.class == type) return Integer.class;
		if (float.class == type) return Float.class;
		if (long.class == type) return Long.class;
		if (double.class == type) return Double.class;
		throw new IllegalArgumentException();
	}

	static Class<?> loadClass(ClassLoader loader, Type stackOwnerType) {
		String name = stackOwnerType.getInternalName().replace('/', '.');
		return doLoadClass(loader, name);
	}

	private static Class<?> doLoadClass(ClassLoader loader, String name) {
		if (name.startsWith("[")) {
			Class<?> aClass = doLoadClass(loader, name.substring(1));
			Class<?> aClass1 = Array.newInstance(aClass, 0).getClass();
			return aClass1;
		}
		if (name.startsWith("L") && name.endsWith(";")) {
			return doLoadClass(loader, name.substring(1, name.length() - 1));
		}
		Class<?> stackOwnerClazz;
		try {
			stackOwnerClazz = loader.loadClass(name);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException(e);
		}
		return stackOwnerClazz;
	}

	static final class IdentityKey<T> {
		private final T value;

		IdentityKey(T value) {this.value = value;}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			IdentityKey<?> ref = (IdentityKey<?>) o;
			return value == ref.value;
		}

		@Override
		public int hashCode() {
			return identityHashCode(value);
		}
	}

	static Class<?> normalizeClass(Class<?> clazz) {
		return clazz.isAnonymousClass() ?
				clazz.getSuperclass() != Object.class ?
						clazz.getSuperclass() :
						clazz.getInterfaces()[0] :
				clazz;
	}

	public static String internalizeClassName(String type) {
		return type.startsWith("[") ? type : "L" + type + ";";
	}

}
