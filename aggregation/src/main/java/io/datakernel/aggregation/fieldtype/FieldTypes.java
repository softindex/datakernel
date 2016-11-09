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

package io.datakernel.aggregation.fieldtype;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import io.datakernel.serializer.StringFormat;
import io.datakernel.serializer.asm.*;
import org.joda.time.Days;
import org.joda.time.LocalDate;

import java.lang.reflect.Type;
import java.util.Set;

public final class FieldTypes {
	private FieldTypes() {
	}

	public static FieldType<Byte> ofByte() {
		return new FieldType<>(byte.class, new SerializerGenByte());
	}

	public static FieldType<Short> ofShort() {
		return new FieldType<>(short.class, new SerializerGenShort());
	}

	public static FieldType<Integer> ofInt() {
		return new FieldType<>(int.class, new SerializerGenInt(true));
	}

	public static FieldType<Long> ofLong() {
		return new FieldType<>(long.class, new SerializerGenLong(true));
	}

	public static FieldType<Float> ofFloat() {
		return new FieldType<>(float.class, new SerializerGenFloat());
	}

	public static FieldType<Double> ofDouble() {
		return new FieldType<>(double.class, new SerializerGenDouble());
	}

	public static <T> FieldType<T> ofSet(FieldType<T> fieldType) {
		SerializerGenSet serializer = new SerializerGenSet(fieldType.getSerializer());
		Type dataType = new TypeToken<Set<T>>() {}
				.where(new TypeParameter<T>() {}, (TypeToken<T>) TypeToken.of(fieldType.getDataType()))
				.getType();
		return new FieldType<>(Set.class, dataType, serializer);
	}

	public static <T extends Enum<T>> FieldType<T> ofEnum(Class<T> enumClass) {
		return new FieldType<>(enumClass, new SerializerGenEnum(enumClass));
	}

	public static FieldType<String> ofString() {
		return new FieldType<>(String.class, new SerializerGenString());
	}

	public static FieldType<String> ofString(StringFormat format) {
		return new FieldType<>(String.class, new SerializerGenString(format));
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
			super(int.class, LocalDate.class, new SerializerGenInt(true));
			this.startDate = startDate;
		}

		@Override
		public LocalDate toValue(Object internalValue) {
			return startDate.plusDays((Integer) internalValue);
		}

		@Override
		public Object toInternalValue(LocalDate value) {
			return Days.daysBetween(startDate, value).getDays();
		}

	}
}
