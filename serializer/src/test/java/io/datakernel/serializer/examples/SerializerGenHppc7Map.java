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

package io.datakernel.serializer.examples;

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.ExpressionParameter;
import io.datakernel.serializer.asm.AbstractSerializerGenMap;
import io.datakernel.serializer.asm.SerializerGen;

import java.util.function.Function;

import static io.datakernel.serializer.examples.SerializerBuilderUtils.capitalize;

public final class SerializerGenHppc7Map extends AbstractSerializerGenMap {
	// region creators
	public SerializerGenHppc7Map(SerializerGen keySerializer, SerializerGen valueSerializer, Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType, boolean nullable) {
		super(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType, nullable);
	}

	public SerializerGenHppc7Map(SerializerGen keySerializer, SerializerGen valueSerializer, Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType) {
		this(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType, false);
	}
	// endregion

	@Override
	protected Expression mapForEach(Expression collection, Function<ExpressionParameter, Expression> key, Function<ExpressionParameter, Expression> value) {
		try {
			String prefix = capitalize(keyType.getSimpleName()) + capitalize(valueType.getSimpleName());
			Class<?> iteratorType = Class.forName("com.carrotsearch.hppc.cursors." + prefix + "Cursor");
			return new ForEachHppcMap(collection, ExpressionParameter.bind("value", value), ExpressionParameter.bind("key", key), iteratorType);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("There is no hppc cursor for " + keyType.getSimpleName(), e);
		}
	}

	@Override
	public SerializerGen asNullable() {
		return new SerializerGenHppc7Map(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType, true);
	}
}
