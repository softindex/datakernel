package io.datakernel.serializer;

import io.datakernel.codegen.utils.Preconditions;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public final class SimpleType {
	private final Class clazz;
	private final SimpleType[] typeParams;

	private SimpleType(final Class clazz, final SimpleType[] typeParams) {
		this.clazz = clazz;
		this.typeParams = typeParams;
	}

	public static SimpleType of(final Class clazz, final SimpleType... typeParams) {
		return new SimpleType(clazz, typeParams);
	}

	public static SimpleType ofClass(final Class clazz) {
		return new SimpleType(clazz, new SimpleType[]{});
	}

	public static SimpleType ofClass(final Class clazz, final Class... typeParams) {
		return new SimpleType(clazz, Arrays.stream(typeParams)
				.map(SimpleType::ofClass)
				.collect(toList())
				.toArray(new SimpleType[]{}));
	}

	public static SimpleType ofType(final Type type) {
		if (type instanceof Class) {
			return ofClass(((Class) type));
		} else if (type instanceof ParameterizedType) {
			final ParameterizedType parameterizedType = (ParameterizedType) type;
			return of(((Class) parameterizedType.getRawType()),
					Arrays.stream(parameterizedType.getActualTypeArguments())
							.map(SimpleType::ofType)
							.collect(toList())
							.toArray(new SimpleType[]{}));
		} else if (type instanceof WildcardType) {
			final Type[] upperBounds = ((WildcardType) type).getUpperBounds();
			Preconditions.check(upperBounds.length == 1, type);
			return ofType(upperBounds[0]);
		} else {
			throw new IllegalArgumentException(type.getTypeName());
		}
	}

	public Class getClazz() {
		return clazz;
	}

	public SimpleType[] getTypeParams() {
		return typeParams;
	}

	public Type getType() {
		if (typeParams.length == 0) return clazz;

		final Type[] types = Arrays.stream(typeParams)
				.map(SimpleType::getType)
				.collect(toList())
				.toArray(new Type[]{});

		return new ParameterizedType() {
			@Override
			public Type[] getActualTypeArguments() {
				return types;
			}

			@Override
			public Type getRawType() {
				return clazz;
			}

			@Override
			public Type getOwnerType() {
				return null;
			}

			@Override
			public String toString() {
				return SimpleType.this.toString();
			}
		};
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		final SimpleType that = (SimpleType) o;

		if (!clazz.equals(that.clazz)) return false;
		// Probably incorrect - comparing Object[] arrays with Arrays.equals
		return Arrays.equals(typeParams, that.typeParams);
	}

	@Override
	public int hashCode() {
		int result = clazz.hashCode();
		result = 31 * result + Arrays.hashCode(typeParams);
		return result;
	}

	@Override
	public String toString() {
		return clazz.getName() + (typeParams.length == 0 ? "" :
				Arrays.stream(typeParams)
						.map(Object::toString)
						.collect(Collectors.joining(",", "<", ">")));
	}
}
