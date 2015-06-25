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

package io.datakernel.cube.dimensiontype;

import io.datakernel.serializer.asm.SerializerGen;

import java.util.Comparator;

/**
 * Represents a type of cube dimension. It can be enumerable (integer, for example) or not (string or floating-point number).
 */
public abstract class DimensionType implements Comparator<Object> {
	protected final Class<?> dataType;

	public DimensionType(Class<?> dataType) {
		this.dataType = dataType;
	}

	public abstract SerializerGen serializerGen();

	public Class<?> getDataType() {
		return dataType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DimensionType that = (DimensionType) o;

		if (!dataType.equals(that.dataType)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return dataType.hashCode();
	}

	public String toString(Object o) {
		return o.toString();
	}

	public abstract Object toInternalRepresentation(String o);

	@Override
	public String toString() {
		return "{" + dataType + '}';
	}
}

