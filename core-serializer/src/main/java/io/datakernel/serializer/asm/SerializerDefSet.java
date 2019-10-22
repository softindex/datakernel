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

import io.datakernel.codegen.Expression;

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static io.datakernel.codegen.Expressions.callStatic;
import static io.datakernel.codegen.Expressions.value;

public class SerializerDefSet extends AbstractSerializerDefCollection {
	public SerializerDefSet(SerializerDef valueSerializer) {
		this(valueSerializer, false);
	}

	private SerializerDefSet(SerializerDef valueSerializer, boolean nullable) {
		super(valueSerializer, Set.class,
				valueSerializer.getRawType().isEnum() ?
						EnumSet.class :
						LinkedHashSet.class,
				Object.class, nullable);
	}

	@Override
	protected Expression createConstructor(Expression length) {
		if (valueSerializer.getRawType().isEnum()) {
			return callStatic(EnumSet.class, "noneOf", value(valueSerializer.getRawType()));
		}
		return super.createConstructor(length);
	}

	@Override
	public SerializerDef withNullable() {
		return new SerializerDefSet(valueSerializer, true);
	}
}
