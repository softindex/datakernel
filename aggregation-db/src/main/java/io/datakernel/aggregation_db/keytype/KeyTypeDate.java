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

package io.datakernel.aggregation_db.keytype;

import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.asm.SerializerGenInt;
import org.joda.time.Days;
import org.joda.time.LocalDate;

public final class KeyTypeDate extends KeyType implements KeyTypeEnumerable {
	private final LocalDate startDate;

	KeyTypeDate() {
		this(LocalDate.parse("1970-01-01"));
	}

	KeyTypeDate(LocalDate startDate) {
		super(int.class);
		this.startDate = startDate;
	}

	@Override
	public SerializerGen serializerGen() {
		return new SerializerGenInt(true);
	}

	@Override
	public Object getPrintable(Object value) {
		return startDate.plusDays((Integer) value);
	}

	@Override
	public Object fromString(String str) {
		LocalDate date = LocalDate.parse(str);
		return Days.daysBetween(startDate, date).getDays();
	}

	@Override
	public int compare(Object o1, Object o2) {
		return ((Integer) o1).compareTo((Integer) o2);
	}

	@Override
	public Object increment(Object object) {
		Integer integerToIncrement = (Integer) object;
		return ++integerToIncrement;
	}

	@Override
	public long difference(Object o1, Object o2) {
		return ((Integer) o1) - ((Integer) o2);
	}
}

