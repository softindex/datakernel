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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.datakernel.codegen.Expressions.*;
import static org.objectweb.asm.Type.getType;

public final class SerializerDefMap extends AbstractSerializerDefMap {
	private SerializerDefMap(SerializerDef keySerializer, SerializerDef valueSerializer, boolean nullable) {
		super(keySerializer, valueSerializer, Map.class,
				keySerializer.getRawType().isEnum() ?
						EnumMap.class :
						HashMap.class,
				Object.class, Object.class, nullable);
	}

	public SerializerDefMap(SerializerDef keySerializer, SerializerDef valueSerializer) {
		this(keySerializer, valueSerializer, false);
	}

	@Override
	protected Expression createConstructor(Expression length) {
		Class<?> rawType = keySerializer.getRawType();
		if (rawType.isEnum()) {
			return constructor(EnumMap.class, cast(value(getType(rawType)), Class.class));
		}
		return super.createConstructor(length);
	}

	@Override
	public Expression mapForEach(Expression collection, Function<Expression, Expression> forEachKey, Function<Expression, Expression> forEachValue) {
		return forEach(collection, forEachKey, forEachValue);
	}

	@Override
	public SerializerDef withNullable() {
		return new SerializerDefMap(keySerializer, valueSerializer, true);
	}
}
