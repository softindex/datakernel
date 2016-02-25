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

import java.util.Comparator;

/**
 * Represents a type of aggregation key. It can be enumerable (integer, for example) or not (string or floating-point number).
 */
public abstract class KeyType implements Comparator<Object> {
	protected final Class<?> dataType;
	private final Object restrictedValue;

	public KeyType(Class<?> dataType) {
		this(dataType, null);
	}

	public KeyType(Class<?> dataType, Object restrictedValue) {
		this.dataType = dataType;
		this.restrictedValue = restrictedValue;
	}

	public abstract SerializerGen serializerGen();

	public Class<?> getDataType() {
		return dataType;
	}

	public Object getRestrictedValue() {
		return restrictedValue;
	}

	public Object getPrintable(Object value) {
		return value;
	}

	public abstract Object fromString(String str);

	@Override
	public String toString() {
		return "{" + dataType + '}';
	}
}

