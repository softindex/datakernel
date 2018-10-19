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

import io.datakernel.codegen.AbstractExpressionMapForEach;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.VarLocal;

import static io.datakernel.codegen.Expressions.property;

public final class ForEachHppcMap extends AbstractExpressionMapForEach {

	public ForEachHppcMap(Expression collection, Expression forValue, Expression forKey, Class<?> entryType) {
		super(collection, forKey, forValue, entryType);
	}

	@Override
	protected Expression getEntries() {
		return collection;
	}

	@Override
	protected Expression getKey(VarLocal entry) {
		return property(entry, "key");
	}

	@Override
	protected Expression getValue(VarLocal entry) {
		return property(entry, "value");
	}
}
