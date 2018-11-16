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

package io.datakernel.aggregation.fieldtype;

import com.google.gson.TypeAdapter;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.utils.Primitives;
import io.datakernel.json.GsonAdapters;
import io.datakernel.serializer.StringFormat;
import io.datakernel.serializer.asm.*;
import io.datakernel.util.RecursiveType;

import java.lang.reflect.Type;
import java.time.LocalDate;
import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.json.GsonAdapters.*;
import static java.time.temporal.ChronoUnit.DAYS;

public final class FieldTypes {
	private FieldTypes() {
	}

	public static FieldType<Byte> ofByte() {
		return new FieldType<>(byte.class, new SerializerGenByte(), BYTE_JSON);
	}

	public static FieldType<Short> ofShort() {
		return new FieldType<>(short.class, new SerializerGenShort(), SHORT_JSON);
	}

	public static FieldType<Integer> ofInt() {
		return new FieldType<>(int.class, new SerializerGenInt(true), INTEGER_JSON);
	}

	public static FieldType<Long> ofLong() {
		return new FieldType<>(long.class, new SerializerGenLong(true), LONG_JSON);
	}

	public static FieldType<Float> ofFloat() {
		return new FieldType<>(float.class, new SerializerGenFloat(), FLOAT_JSON);
	}

	public static FieldType<Double> ofDouble() {
		return new FieldType<>(double.class, new SerializerGenDouble(), DOUBLE_JSON);
	}

	public static <T> FieldType<Set<T>> ofSet(FieldType<T> fieldType) {
		SerializerGenSet serializer = new SerializerGenSet(fieldType.getSerializer());
		Type wrappedNestedType = fieldType.getDataType() instanceof Class ?
				Primitives.wrap((Class<?>) fieldType.getDataType()) :
				fieldType.getDataType();
		Type dataType = RecursiveType.of(Set.class, RecursiveType.of(wrappedNestedType)).getType();
		TypeAdapter<Set<T>> json = GsonAdapters.ofSet(fieldType.getJson());
		return new FieldType<>(Set.class, dataType, serializer, json, json);
	}

	public static <E extends Enum<E>> FieldType<E> ofEnum(Class<E> enumClass) {
		return new FieldType<>(enumClass, new SerializerGenEnum(enumClass), GsonAdapters.ofEnum(enumClass));
	}

	public static FieldType<String> ofString() {
		return new FieldType<>(String.class, new SerializerGenString(), STRING_JSON);
	}

	public static FieldType<String> ofString(StringFormat format) {
		return new FieldType<>(String.class, new SerializerGenString(format), STRING_JSON);
	}

	public static FieldType<LocalDate> ofLocalDate() {
		return new FieldTypeDate();
	}

	public static FieldType<LocalDate> ofLocalDate(LocalDate startDate) {
		return new FieldTypeDate(startDate);
	}

	private static final class FieldTypeDate extends FieldType<LocalDate> {
		private final LocalDate startDate;

		FieldTypeDate() {
			this(LocalDate.parse("1970-01-01"));
		}

		FieldTypeDate(LocalDate startDate) {
			super(int.class, LocalDate.class, new SerializerGenInt(true), LOCAL_DATE_JSON, INTEGER_JSON);
			this.startDate = startDate;
		}

		@Override
		public Expression toValue(Expression internalValue) {
			return call(value(startDate), "plusDays", cast(internalValue, long.class));
		}

		@Override
		public Object toInternalValue(LocalDate value) {
			return (int) DAYS.between(startDate, value);
		}

	}
}
