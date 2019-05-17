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

	public Class<?> getRawType() {
		if (type instanceof Class) {
			return (Class<?>) type;
		} else if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			return (Class<?>) parameterizedType.getRawType();
		} else {
			throw new IllegalArgumentException(type.getTypeName());
		}
	}
}
