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

import io.datakernel.aggregation_db.processor.FieldProcessor;
import io.datakernel.serializer.asm.SerializerGen;

/**
 * Represents a type of aggregation field.
 */
public abstract class FieldType {
	private final Class<?> dataType;

	@Override
	public String toString() {
		return "{" + dataType + '}';
	}

	public FieldType(Class<?> dataType) {
		this.dataType = dataType;
	}

	public Class<?> getDataType() {
		return dataType;
	}

	public abstract FieldProcessor fieldProcessor();

	public abstract SerializerGen serializerGen();

	public Object getPrintable(Object value) {
		return value;
	}
}
