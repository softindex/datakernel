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

package io.datakernel.aggregation_db.fieldtype;

import io.datakernel.codegen.utils.Primitives;
import io.datakernel.serializer.asm.SerializerGen;

import java.lang.reflect.Type;

/**
 * Represents a type of aggregation field.
 */
public class FieldType<T> {
	private final Class<?> internalDataType;
	private final Type dataType;
	private final SerializerGen serializer;

	protected FieldType(Class<?> internalDataType, Type dataType, SerializerGen serializer) {
		this.internalDataType = internalDataType;
		this.dataType = dataType;
		this.serializer = serializer;
	}

	protected FieldType(Class<T> dataType, SerializerGen serializer) {
		this.internalDataType = dataType;
		this.dataType = dataType;
		this.serializer = serializer;
	}

	public final Class<?> getInternalDataType() {
		return internalDataType;
	}

	public final Type getDataType() {
		if (dataType instanceof Class<?>) {
			return Primitives.wrap((Class<Object>) dataType);
		}
		return dataType;
	}

	public SerializerGen getSerializer() {
		return serializer;
	}

	public T toValue(Object internalValue) {
		return (T) internalValue;
	}

	public Object toInternalValue(T value) {
		return value;
	}

	@Override
	public String toString() {
		return "{" + internalDataType + '}';
	}

	public interface FieldConverters {
		Object toInternalValue(String field, Object value);

		Object toValue(String field, Object internalValue);
	}

	public static FieldConverters identityConverter() {
		return new FieldConverters() {
			@Override
			public Object toInternalValue(String field, Object value) {
				return value;
			}

			@Override
			public Object toValue(String field, Object internalValue) {
				return internalValue;
			}
		};
	}

}
