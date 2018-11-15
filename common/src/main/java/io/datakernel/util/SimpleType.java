/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.util;

import io.datakernel.annotation.Nullable;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public final class SimpleType {
	private final Class clazz;
	private final SimpleType[] typeParams;
	private final int arrayDimension;

	private SimpleType(Class clazz, SimpleType[] typeParams, int arrayDimension) {
		this.clazz = clazz;
		this.typeParams = typeParams;
		this.arrayDimension = arrayDimension;
	}

	public static SimpleType of(Class clazz, SimpleType... typeParams) {
		return new SimpleType(clazz, typeParams, 0);
	}

	public static SimpleType ofClass(Class clazz) {
		return new SimpleType(clazz, new SimpleType[]{}, clazz.isArray() ? 1 : 0);
	}

	public static SimpleType ofClass(Class clazz, Class... typeParams) {
		return new SimpleType(clazz, Arrays.stream(typeParams)
				.map(SimpleType::ofClass)
				.collect(toList())
				.toArray(new SimpleType[]{}), clazz.isArray() ? 1 : 0);
	}

	public static SimpleType ofType(Type type) {
		if (type instanceof Class) {
			return ofClass(((Class) type));
		} else if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			return of(((Class) parameterizedType.getRawType()),
					Arrays.stream(parameterizedType.getActualTypeArguments())
							.map(SimpleType::ofType)
							.collect(toList())
							.toArray(new SimpleType[]{}));
		} else if (type instanceof WildcardType) {
			Type[] upperBounds = ((WildcardType) type).getUpperBounds();
			Preconditions.check(upperBounds.length == 1, type);
			return ofType(upperBounds[0]);
		} else if (type instanceof GenericArrayType) {
			SimpleType component = ofType(((GenericArrayType) type).getGenericComponentType());
			return new SimpleType(component.clazz, component.typeParams, component.arrayDimension + 1);
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

			@Nullable
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
		// Probably incorrect - comparing Object[] arrays with Arrays.equals
		return Arrays.equals(typeParams, that.typeParams);
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
