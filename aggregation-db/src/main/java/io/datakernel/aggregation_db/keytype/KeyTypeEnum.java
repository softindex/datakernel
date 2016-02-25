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
import io.datakernel.serializer.asm.SerializerGenEnum;

public final class KeyTypeEnum<T extends Enum<T>> extends KeyType {
	private Class<T> enumClass;

	KeyTypeEnum(Class<T> enumClass, Object restrictedValue) {
		super(enumClass, restrictedValue);
		this.enumClass = enumClass;
	}

	@Override
	public SerializerGen serializerGen() {
		return new SerializerGenEnum(enumClass);
	}

	@Override
	public Object getPrintable(Object value) {
		return ((Enum<?>) value).name();
	}

	@Override
	public Object fromString(String str) {
		return Enum.valueOf(enumClass, str);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int compare(Object enum1, Object enum2) {
		return ((Enum) enum1).compareTo((Enum) enum2);
	}
}
