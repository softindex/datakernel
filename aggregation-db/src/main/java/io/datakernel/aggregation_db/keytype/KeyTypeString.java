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

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import io.datakernel.serializer.StringFormat;
import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.asm.SerializerGenString;

public class KeyTypeString extends KeyType {
	private final StringFormat format;

	public KeyTypeString() {
		this(null);
	}

	public KeyTypeString(Object restrictedValue) {
		this(null, restrictedValue);
	}

	public KeyTypeString(StringFormat format) {
		this(format, null);
	}

	public KeyTypeString(StringFormat format, Object restrictedValue) {
		super(String.class, restrictedValue);
		this.format = format;
	}

	@Override
	public SerializerGen serializerGen() {
		SerializerGenString serializer = new SerializerGenString();
		if (format != null) {
			serializer.encoding(format);
		}
		return serializer;
	}

	@Override
	public JsonPrimitive toJson(Object value) {
		return new JsonPrimitive((String) value);
	}

	@Override
	public Object fromJson(JsonElement value) {
		return value.getAsString();
	}

	@Override
	public int compare(Object o1, Object o2) {
		return ((String) o1).compareTo((String) o2);
	}
}
