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

package io.datakernel.serializer.impl;

import io.datakernel.codegen.Expression;
import io.datakernel.serializer.SerializerDef;

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static io.datakernel.codegen.Expressions.staticCall;
import static io.datakernel.codegen.Expressions.value;

public final class SerializerDefSet extends AbstractSerializerDefCollection {
	public SerializerDefSet(SerializerDef valueSerializer) {
		this(valueSerializer, false);
	}

	private SerializerDefSet(SerializerDef valueSerializer, boolean nullable) {
		super(valueSerializer, Set.class,
				valueSerializer.getDecodeType().isEnum() ?
						EnumSet.class :
						LinkedHashSet.class,
				Object.class, nullable);
	}

	@Override
	protected Expression createConstructor(Expression length) {
		if (valueSerializer.getDecodeType().isEnum()) {
			return staticCall(EnumSet.class, "noneOf", value(valueSerializer.getEncodeType()));
		}
		return super.createConstructor(length);
	}

	@Override
	public SerializerDef ensureNullable() {
		return new SerializerDefSet(valueSerializer, true);
	}
}
