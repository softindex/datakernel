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

package io.datakernel.serializer.impl;

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializerDef;

import static io.datakernel.serializer.impl.SerializerExpressions.readByte;
import static io.datakernel.serializer.impl.SerializerExpressions.writeByte;

public final class SerializerDefByte extends SerializerDefPrimitive {
	public SerializerDefByte() {
		this(true);
	}

	public SerializerDefByte(boolean wrapped) {
		super(byte.class, wrapped);
	}

	@Override
	public SerializerDef ensureWrapped() {
		return new SerializerDefByte(true);
	}

	@Override
	protected Expression doSerialize(Expression byteArray, Variable off, Expression value, CompatibilityLevel compatibilityLevel) {
		return writeByte(byteArray, off, value);
	}

	@Override
	protected Expression doDeserialize(Expression in, CompatibilityLevel compatibilityLevel) {
		return readByte(in);
	}
}
