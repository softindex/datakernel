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

package io.datakernel.serializer.asm;

import java.util.ArrayList;
import java.util.List;

public final class SerializerDefList extends AbstractSerializerDefCollection {

	public SerializerDefList(SerializerDef valueSerializer) {
		this(valueSerializer, false);
	}

	private SerializerDefList(SerializerDef valueSerializer, boolean nullable) {
		super(valueSerializer, List.class, ArrayList.class, Object.class, nullable);
	}

	@Override
	public SerializerDef withNullable() {
		return new SerializerDefList(valueSerializer, true);
	}
}
