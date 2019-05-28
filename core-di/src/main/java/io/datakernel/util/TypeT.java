package io.datakernel.util;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class TypeT<T> {
	@NotNull
	private final Type type;

	public TypeT() {
		this.type = getSuperclassTypeParameter(this.getClass());
	}

	private TypeT(@NotNull Type type) {
		this.type = type;
	}

	@NotNull
	public static <T> TypeT<T> ofType(@NotNull Type type) {
		return new TypeT<T>(type) {};
	}

	@NotNull
	public static <T> TypeT<T> of(@NotNull Class<T> type) {
		return new TypeT<T>(type) {};
	}

	@NotNull
	private static Type getSuperclassTypeParameter(@NotNull Class<?> subclass) {
		Type superclass = subclass.getGenericSuperclass();
		if (superclass instanceof ParameterizedType) {
			return ((ParameterizedType) superclass).getActualTypeArguments()[0];
		}
		throw new IllegalArgumentException("Unsupported type: " + superclass);
	}

	@NotNull
	public Type getType() {
		return type;
	}

	@SuppressWarnings("unchecked")
	public Class<T> getRawType() {
		if (type instanceof Class) {
			return (Class<T>) type;
		} else if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			return (Class<T>) parameterizedType.getRawType();
		} else {
			throw new IllegalArgumentException(type.getTypeName());
		}
	}

	public String getDisplayString() {
		return type.getTypeName().replaceAll("(?:\\w+\\.)*(\\w+)", "$1");
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof TypeT)) {
			return false;
		}

		TypeT<?> typeT = (TypeT<?>) o;

		return type.equals(typeT.type);
	}

	@Override
	public int hashCode() {
		return type.hashCode();
	}

	@Override
	public String toString() {
		return type.getTypeName();
	}
}
