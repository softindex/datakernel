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

package io.datakernel.serializer.asm;

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;

import static io.datakernel.serializer.asm.SerializerExpressions.readChar;
import static io.datakernel.serializer.asm.SerializerExpressions.writeChar;

public final class SerializerGenChar extends SerializerGenPrimitive {
	public SerializerGenChar() {
		super(char.class);
	}

	@Override
	protected Expression doSerialize(Expression byteArray, Variable off, Expression value) {
		return writeChar(byteArray, off, value);
	}

	@Override
	protected Expression doDeserialize(Expression byteArray, Variable off) {
		return readChar(byteArray, off);
	}
}
