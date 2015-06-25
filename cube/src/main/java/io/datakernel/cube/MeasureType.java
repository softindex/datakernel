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

package io.datakernel.cube;

import io.datakernel.serializer.asm.*;

/**
 * Represents a type of cube measure. Measure is numeric and arithmetically additive.
 */
public final class MeasureType {
	public static final MeasureType SUM_INT = new MeasureType(int.class);
	public static final MeasureType SUM_LONG = new MeasureType(long.class);
	public static final MeasureType SUM_FLOAT = new MeasureType(float.class);
	public static final MeasureType SUM_DOUBLE = new MeasureType(double.class);

	private final Class<?> dataType;

	@Override
	public String toString() {
		return "{" + dataType + '}';
	}

	public MeasureType(Class<?> dataType) {
		this.dataType = dataType;
	}

	public Class<?> getDataType() {
		return dataType;
	}

	public SerializerGen serializerGen() {
		SerializerGen result;
		if (dataType == int.class)
			result = new SerializerGenInt(true);
		else if (dataType == long.class)
			result = new SerializerGenLong(true);
		else if (dataType == float.class)
			result = new SerializerGenFloat();
		else if (dataType == double.class)
			result = new SerializerGenDouble();
		else
			throw new IllegalArgumentException();

		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		MeasureType that = (MeasureType) o;

		if (!dataType.equals(that.dataType)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = dataType.hashCode();
		return result;
	}
}
