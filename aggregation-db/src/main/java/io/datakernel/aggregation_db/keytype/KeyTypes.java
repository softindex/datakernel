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

import io.datakernel.serializer.StringFormat;
import org.joda.time.LocalDate;

public final class KeyTypes {
	public static KeyType intKey() {
		return new KeyTypeInt(null);
	}

	public static KeyType intKey(Object restrictedValue) {
		return new KeyTypeInt(restrictedValue);
	}

	public static KeyType longKey() {
		return new KeyTypeLong(null);
	}

	public static KeyType longKey(Object restrictedValue) {
		return new KeyTypeLong(restrictedValue);
	}

	public static KeyType byteKey() {
		return new KeyTypeByte(null);
	}

	public static KeyType byteKey(Object restrictedValue) {
		return new KeyTypeInt(restrictedValue);
	}

	public static <T extends Enum<T>> KeyType enumKey(Class<T> enumClass) {
		return new KeyTypeEnum<>(enumClass, null);
	}

	public static <T extends Enum<T>> KeyType enumKey(Class<T> enumClass, Object restrictedValue) {
		return new KeyTypeEnum<>(enumClass, restrictedValue);
	}

	public static KeyType stringKey() {
		return new KeyTypeString(null, null);
	}

	public static KeyType stringKey(Object restrictedValue) {
		return new KeyTypeString(null, restrictedValue);
	}

	public static KeyType stringKey(StringFormat format) {
		return new KeyTypeString(format, null);
	}

	public static KeyType stringKey(StringFormat format, Object restrictedValue) {
		return new KeyTypeString(format, restrictedValue);
	}

	public static KeyType dateKey() {
		return new KeyTypeDate();
	}

	public static KeyType dateKey(LocalDate startDate) {
		return new KeyTypeDate(startDate);
	}
}
