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

import static io.datakernel.codegen.Expressions.*;

public class SerializerGenSet extends AbstractSerializerGenCollection {
	// region creators
	public SerializerGenSet(SerializerGen valueSerializer, boolean nullable) {
		super(valueSerializer, Set.class,
				valueSerializer.getRawType().isEnum() ?
						EnumSet.class :
						LinkedHashSet.class,
				Object.class, nullable);
	}

	public SerializerGenSet(SerializerGen valueSerializer) {
		this(valueSerializer, false);
	}
	// endregion

	@Override
	protected Expression createConstructor(Expression length) {
		if (valueSerializer.getRawType().isEnum()) {
			return let(callStatic(EnumSet.class, "noneOf", value(valueSerializer.getRawType())));
		}
		return super.createConstructor(length);
	}

	@Override
	public SerializerGen asNullable() {
		return new SerializerGenSet(valueSerializer, true);
	}
}
