package io.datakernel.util;

import io.datakernel.annotation.Nullable;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public final class SimpleType {
	private static final SimpleType[] NO_TYPE_PARAMS = new SimpleType[0];

	private final Class clazz;
	private final SimpleType[] typeParams;
	private final int arrayDimension;

	private SimpleType(Class clazz, SimpleType[] typeParams, int arrayDimension) {
		this.clazz = clazz;
		this.typeParams = typeParams;
		this.arrayDimension = arrayDimension;
	}

	public static SimpleType of(Class clazz) {
		return new SimpleType(clazz, NO_TYPE_PARAMS, clazz.isArray() ? 1 : 0);
	}

	public static SimpleType of(Class clazz, SimpleType... typeParams) {
		return new SimpleType(clazz, typeParams, clazz.isArray() ? 1 : 0);
	}

	public static SimpleType of(Class clazz, List<SimpleType> typeParams) {
		return new SimpleType(clazz, typeParams.toArray(new SimpleType[0]), clazz.isArray() ? 1 : 0);
	}

	public static SimpleType of(TypeT<?> type) {
		return of(type.getType());
	}

	public static SimpleType of(Type type) {
		if (type instanceof Class) {
			return of(((Class) type));
		} else if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			return of(((Class) parameterizedType.getRawType()),
					Arrays.stream(parameterizedType.getActualTypeArguments())
							.map(SimpleType::of)
							.collect(toList())
							.toArray(new SimpleType[0]));
		} else if (type instanceof WildcardType) {
			Type[] upperBounds = ((WildcardType) type).getUpperBounds();
			Preconditions.check(upperBounds.length == 1, type);
			return of(upperBounds[0]);
		} else if (type instanceof GenericArrayType) {
			SimpleType component = of(((GenericArrayType) type).getGenericComponentType());
			return new SimpleType(component.clazz, component.typeParams, component.arrayDimension + 1);
		} else {
			throw new IllegalArgumentException(type.getTypeName());
		}
	}

	public Class getRawType() {
		return clazz;
	}

	public SimpleType[] getTypeParams() {
		return typeParams;
	}

	public boolean isArray() {
		return arrayDimension != 0;
	}

	public int getArrayDimension() {
		return arrayDimension;
	}

	private Type getArrayType(Type component, int arrayDeepness) {
		if (arrayDeepness == 0) {
			return component;
		}
		return (GenericArrayType) () -> getArrayType(component, arrayDeepness - 1);
	}

	public Type getType() {
		if (typeParams.length == 0) {
			return getArrayType(clazz, arrayDimension);
		}

		Type[] types = Arrays.stream(typeParams)
				.map(SimpleType::getType)
				.collect(toList())
				.toArray(new Type[]{});

		ParameterizedType parameterized = new ParameterizedType() {
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
		return getArrayType(parameterized, arrayDimension);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SimpleType that = (SimpleType) o;
		if (!clazz.equals(that.clazz)) return false;
		if (!Arrays.equals(typeParams, that.typeParams)) return false;
		return true;
	}

	@Override
	public int hashCode() {
		int result = clazz.hashCode();
		result = 31 * result + Arrays.hashCode(typeParams);
		return result;
	}

	public String getSimpleName() {
		return clazz.getSimpleName() + (typeParams.length == 0 ? "" :
				Arrays.stream(typeParams)
						.map(SimpleType::getSimpleName)
						.collect(Collectors.joining(",", "<", ">"))) + (new String(new char[arrayDimension]).replace("\0", "[]"));
	}

	public String getName() {
		return clazz.getName() + (typeParams.length == 0 ? "" :
				Arrays.stream(typeParams)
						.map(SimpleType::getName)
						.collect(Collectors.joining(",", "<", ">"))) + (new String(new char[arrayDimension]).replace("\0", "[]"));
	}

	@Nullable
	public String getPackage() {
		Package pkg = clazz.getPackage();
		return pkg != null ? pkg.getName() : null;
	}

	@Override
	public String toString() {
		return getName();
	}
}
