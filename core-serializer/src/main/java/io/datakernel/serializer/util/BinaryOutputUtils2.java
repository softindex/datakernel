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

package io.datakernel.serializer.util;

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;

import static io.datakernel.codegen.Expressions.*;

/**
 * Provides methods for writing primitives
 * and Strings to byte arrays
 */
public final class BinaryOutputUtils2 {

	public static Expression write(Expression buf, Variable pos, Expression bytes) {
		return write(buf, pos, bytes, value(0), call(buf, "length"));
	}

	public static Expression write(Expression buf, Variable pos, Expression bytes, Expression bytesOff, Expression bytesLen) {
		return sequence(
				callStatic(System.class, "arraycopy", buf, pos, bytes, bytesOff, buf, pos, bytesLen),
				set(pos, add(pos, bytesLen)),
				pos);
	}

	public static int write(byte[] buf, int off, byte[] bytes, int bytesOff, int len) {
		System.arraycopy(bytes, bytesOff, buf, off, len);
		return off + len;
	}

	public static Expression writeBoolean(Expression buf, Variable pos, Expression value) {
		return writeByte(buf, pos, value);
	}

	public static Expression writeByte(Expression buf, Variable pos, Expression value) {
		return sequence(
				setArrayItem(buf, pos, value),
				set(pos, inc(pos)),
				pos);
	}

	public static Expression writeInt(Expression buf, Variable pos, Expression value) {
		return sequence(
				setArrayItem(buf, pos, cast(ushr(value, value(24)), byte.class)),
				setArrayItem(buf, add(pos, value(1)), cast(ushr(value, value(16)), byte.class)),
				setArrayItem(buf, add(pos, value(2)), cast(ushr(value, value(8)), byte.class)),
				setArrayItem(buf, add(pos, value(3)), cast(value, byte.class)),
				set(pos, add(pos, value(4))),
				pos);
	}

	public static Expression writeVarInt(Expression buf, Variable pos, Expression value) {
		return doWriteVarInt(buf, pos, value, 0);
	}

	private static Expression doWriteVarInt(Expression buf, Variable pos, Expression value, int nesting) {
		return ifThenElse(
				cmpEq(and(value, value(~0x7F)), value(0)),
				sequence(
						setArrayItem(buf, add(pos, value(nesting)), cast(value, byte.class)),
						set(pos, add(pos, value(nesting + 1))),
						pos),
				sequence(
						setArrayItem(buf, add(pos, value(nesting)), cast(or(value, value(0x80)), byte.class)),
						nesting != 3 ?
								doWriteVarInt(buf, pos, ushr(value, value(7)), nesting + 1) :
								sequence(
										setArrayItem(buf, add(pos, value(nesting + 1)), cast(value, byte.class)),
										set(pos, add(pos, value(nesting + 2))),
										pos)
				));
	}

}
