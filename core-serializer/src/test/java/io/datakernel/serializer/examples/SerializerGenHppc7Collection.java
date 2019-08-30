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
import io.datakernel.serializer.asm.AbstractSerializerGenCollection;
import io.datakernel.serializer.asm.SerializerGen;

import java.util.function.Function;

import static io.datakernel.serializer.examples.SerializerBuilderUtils.capitalize;

public final class SerializerGenHppc7Collection extends AbstractSerializerGenCollection {
	// region creators
	public SerializerGenHppc7Collection(SerializerGen valueSerializer, Class<?> collectionType, Class<?> elementType, Class<?> collectionImplType, boolean nullable) {
		super(valueSerializer, collectionType, collectionImplType, elementType, nullable);
	}

	public SerializerGenHppc7Collection(Class<?> collectionType, Class<?> collectionImplType, Class<?> valueType, SerializerGen valueSerializer) {
		this(valueSerializer, collectionType, valueType, collectionImplType, false);
	}
	// endregion


	@Override
	protected Expression collectionForEach(Expression collection, Class<?> valueType, Function<Expression, Expression> value) {
		try {
			String prefix = capitalize(elementType.getSimpleName());
			Class<?> iteratorType = Class.forName("com.carrotsearch.hppc.cursors." + prefix + "Cursor");
			return new ForEachHppcCollection(collection, iteratorType, value);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("There is no hppc cursor for " + elementType.getSimpleName(), e);
		}
	}

	@Override
	public SerializerGen asNullable() {
		return new SerializerGenHppc7Collection(valueSerializer, collectionType, elementType, collectionImplType, true);
	}
}
