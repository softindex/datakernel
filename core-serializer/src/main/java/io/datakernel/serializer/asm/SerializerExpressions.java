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
import io.datakernel.codegen.Expressions;
import io.datakernel.codegen.Variable;

import static io.datakernel.codegen.Expressions.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Provides methods for writing primitives
 * and Strings to byte arrays
 */
public final class SerializerExpressions {

	public static Expression writeBytes(Expression buf, Variable pos, Expression bytes) {
		return writeBytes(buf, pos, bytes, value(0), length(bytes));
	}

	public static Expression writeBytes(Expression buf, Variable pos, Expression bytes, Expression bytesOff, Expression bytesLen) {
		return ensureRemaining(buf, pos, bytesLen,
				sequence(
						callStatic(System.class, "arraycopy", bytes, bytesOff, buf, pos, bytesLen),
						set(pos, add(pos, bytesLen))));
	}

	public static Expression writeByte(Expression buf, Variable pos, Expression value) {
		return ensureRemaining(buf, pos, 1,
				sequence(
						setArrayItem(buf, pos, value),
						set(pos, add(pos, value(1)))));
	}

	public static Expression writeBoolean(Expression buf, Variable pos, Expression value) {
		return writeByte(buf, pos, value);
	}

	public static Expression writeChar(Expression buf, Variable pos, Expression value) {
		return writeShort(buf, pos, value);
	}

	public static Expression writeShort(Expression buf, Variable pos, Expression value) {
		return ensureRemaining(buf, pos, 2,
				sequence(
						setArrayItem(buf, add(pos, value(0)), cast(ushr(value, value(8)), byte.class)),
						setArrayItem(buf, add(pos, value(1)), cast(ushr(value, value(0)), byte.class)),
						set(pos, add(pos, value(2)))));
	}

	public static Expression writeInt(Expression buf, Variable pos, Expression value) {
		return ensureRemaining(buf, pos, 4,
				sequence(
						setArrayItem(buf, add(pos, value(0)), cast(ushr(value, value(24)), byte.class)),
						setArrayItem(buf, add(pos, value(1)), cast(ushr(value, value(16)), byte.class)),
						setArrayItem(buf, add(pos, value(2)), cast(ushr(value, value(8)), byte.class)),
						setArrayItem(buf, add(pos, value(3)), cast(ushr(value, value(0)), byte.class)),
						set(pos, add(pos, value(4)))));
	}

	public static Expression writeLong(Expression buf, Variable pos, Expression value) {
		return ensureRemaining(buf, pos, 8,
				sequence(
						setArrayItem(buf, add(pos, value(0)), cast(ushr(value, value(56)), byte.class)),
						setArrayItem(buf, add(pos, value(1)), cast(ushr(value, value(48)), byte.class)),
						setArrayItem(buf, add(pos, value(2)), cast(ushr(value, value(40)), byte.class)),
						setArrayItem(buf, add(pos, value(3)), cast(ushr(value, value(32)), byte.class)),
						setArrayItem(buf, add(pos, value(4)), cast(ushr(value, value(24)), byte.class)),
						setArrayItem(buf, add(pos, value(5)), cast(ushr(value, value(16)), byte.class)),
						setArrayItem(buf, add(pos, value(6)), cast(ushr(value, value(8)), byte.class)),
						setArrayItem(buf, add(pos, value(7)), cast(ushr(value, value(0)), byte.class)),
						set(pos, add(pos, value(8)))));
	}

	public static Expression writeVarInt(Expression buf, Variable pos, Expression value) {
		return ensureRemaining(buf, pos, 5,
				let(value, v -> writeVarIntImpl(buf, pos, v, 0)));
	}

	private static Expression writeVarIntImpl(Expression buf, Variable pos, Variable v, int n) {
		return n != 4 ?
				ifThenElse(
						cmpEq(and(v, value(~0x7F)), value(0)),
						sequence(
								setArrayItem(buf, add(pos, value(n)), cast(v, byte.class)),
								set(pos, add(pos, value(n + 1)))),
						sequence(
								setArrayItem(buf, add(pos, value(n)), cast(or(v, value(0x80)), byte.class)),
								set(v, ushr(v, value(7))),
								writeVarIntImpl(buf, pos, v, n + 1)
						)
				) :
				sequence(
						setArrayItem(buf, add(pos, value(n)), cast(v, byte.class)),
						set(pos, add(pos, value(n + 1))));
	}

	public static Expression writeVarLong(Expression buf, Variable pos, Expression value) {
		return ensureRemaining(buf, pos, 10,
				let(value, v -> writeVarLongImpl(buf, pos, v, 0)));
	}

	private static Expression writeVarLongImpl(Expression buf, Variable pos, Variable v, int n) {
		return n != 9 ?
				ifThenElse(
						cmpEq(and(v, value(~0x7FL)), value(0L)),
						sequence(
								setArrayItem(buf, add(pos, value(n)), cast(v, byte.class)),
								set(pos, add(pos, value(n + 1)))),
						sequence(
								setArrayItem(buf, add(pos, value(n)), cast(or(v, value(0x80L)), byte.class)),
								set(v, ushr(v, value(7))),
								writeVarLongImpl(buf, pos, v, n + 1)
						)
				) :
				sequence(
						setArrayItem(buf, add(pos, value(n)), cast(v, byte.class)),
						set(pos, add(pos, value(n + 1))));
	}

	public static Expression writeFloat(Expression buf, Variable pos, Expression value) {
		return writeInt(buf, pos, callStatic(Float.class, "floatToIntBits", cast(value, float.class)));
	}

	public static Expression writeDouble(Expression buf, Variable pos, Expression value) {
		return writeLong(buf, pos, callStatic(Double.class, "doubleToLongBits", cast(value, double.class)));
	}

	public static Expression writeUTF8(Expression buf, Variable pos, Expression value) {
		return let(call(value, "getBytes", value(UTF_8)),
				bytes -> sequence(
						writeVarInt(buf, pos, length(bytes)),
						writeBytes(buf, pos, bytes)));
	}

	public static Expression writeUTF8Nullable(Expression buf, Variable pos, Expression value) {
		return ifThenElse(isNull(value),
				writeByte(buf, pos, value((byte) 0)),
				let(call(value, "getBytes", value(UTF_8)),
						bytes -> sequence(
								writeVarInt(buf, pos, add(length(bytes), value(1))),
								writeBytes(buf, pos, bytes))));
	}

	public static Expression writeIso88591(Expression buf, Variable pos, Expression value) {
		return writeIso88591Impl(buf, pos, value, false);
	}

	public static Expression writeIso88591Nullable(Expression buf, Variable pos, Expression value) {
		return ifThenElse(isNull(value),
				writeByte(buf, pos, value((byte) 0)),
				writeIso88591Impl(buf, pos, value, true));
	}

	private static Expression writeIso88591Impl(Expression buf, Variable pos, Expression value, boolean nullable) {
		return let(
				call(value, "length"),
				length -> sequence(
						writeVarInt(buf, pos, nullable ? add(length, value(1)) : length),
						ensureRemaining(buf, pos, length, sequence(
								loop(value(0), length,
										i -> setArrayItem(buf, add(pos, i), cast(call(value, "charAt", i), byte.class))),
								set(pos, add(pos, length))))));
	}

	public static Expression ensureRemaining(Expression buf, Variable pos, int size, Expression next) {
		return ensureRemaining(buf, pos, value(size), next);
	}

	public static Expression ensureRemaining(Expression buf, Variable pos, Expression size, Expression next) {
		return ifThenElse(cmpGt(add(pos, size), length(buf)),
				exception(ArrayIndexOutOfBoundsException.class),
				next);
	}

	public static Expression readBytes(Expression buf, Variable pos, Expression result) {
		return readBytes(buf, pos, result, value(0), length(result));
	}

	public static Expression readBytes(Expression buf, Variable pos, Expression result, Expression resultPos, Expression length) {
		return sequence(
				callStatic(System.class, "arraycopy", buf, pos, result, resultPos, length),
				set(pos, add(pos, length)));
	}

	public static Expression readByte(Expression buf, Variable pos) {
		return let(getArrayItem(buf, pos),
				b -> sequence(
						set(pos, inc(pos)),
						b));
	}

	public static Expression readBoolean(Expression buf, Variable pos) {
		return cast(readByte(buf, pos), boolean.class);
	}

	public static Expression readChar(Expression buf, Variable pos) {
		return cast(readShort(buf, pos), char.class);
	}

	public static Expression readShort(Expression buf, Variable pos) {
		return let(
				or(shl(and(getArrayItem(buf, pos), value(0xFF)), value(8)), and(getArrayItem(buf, add(pos, value(1))), value(0xFF))),
				result -> sequence(
						set(pos, add(pos, value(2))),
						result));
	}

	public static Expression readInt(Expression buf, Variable pos) {
		return let(
				fold(Expressions::or,
						shl(and(getArrayItem(buf, add(pos, value(0))), value(0xFF)), value(24)),
						shl(and(getArrayItem(buf, add(pos, value(1))), value(0xFF)), value(16)),
						shl(and(getArrayItem(buf, add(pos, value(2))), value(0xFF)), value(8)),
						shl(and(getArrayItem(buf, add(pos, value(3))), value(0xFF)), value(0))),
				result -> sequence(
						set(pos, add(pos, value(4))),
						result));
	}

	public static Expression readLong(Expression buf, Variable pos) {
		return let(
				fold(Expressions::or,
						shl(and(getArrayItem(buf, add(pos, value(0))), value(0xFFL)), value(56)),
						shl(and(getArrayItem(buf, add(pos, value(1))), value(0xFFL)), value(48)),
						shl(and(getArrayItem(buf, add(pos, value(2))), value(0xFFL)), value(40)),
						shl(and(getArrayItem(buf, add(pos, value(3))), value(0xFFL)), value(32)),
						shl(and(getArrayItem(buf, add(pos, value(4))), value(0xFFL)), value(24)),
						shl(and(getArrayItem(buf, add(pos, value(5))), value(0xFFL)), value(16)),
						shl(and(getArrayItem(buf, add(pos, value(6))), value(0xFFL)), value(8)),
						shl(and(getArrayItem(buf, add(pos, value(7))), value(0xFFL)), value(0))),
				result -> sequence(
						set(pos, add(pos, value(8))),
						result));
	}

	public static Expression readVarInt(Expression buf, Variable pos) {
		return let(value(0),
				result -> sequence(
						readVarIntImpl(buf, pos, result, 0),
						result));
	}

	private static Expression readVarIntImpl(Expression buf, Variable pos, Variable result, int n) {
		return let(getArrayItem(buf, add(pos, value(n))),
				b -> n < 4 ?
						ifThenElse(cmpGe(b, value((byte) 0)),
								sequence(
										set(result, or(result, shl(and(b, value(0xFF)), value(n * 7)))),
										set(pos, add(pos, value(n + 1)))),
								sequence(
										set(result, or(result, shl(and(b, value(0x7F)), value(n * 7)))),
										readVarIntImpl(buf, pos, result, n + 1))) :
						sequence(
								set(result, or(result, shl(and(b, value(0xFF)), value(n * 7)))),
								set(pos, add(pos, value(n + 1)))));
	}

	public static Expression readVarLong(Expression buf, Variable pos) {
		return let(value(0L),
				result -> sequence(
						readVarLongImpl(buf, pos, result, 0),
						result));
	}

	private static Expression readVarLongImpl(Expression buf, Variable pos, Variable result, int n) {
		return let(getArrayItem(buf, add(pos, value(n))),
				b -> n < 9 ?
						ifThenElse(cmpGe(b, value((byte) 0)),
								sequence(
										set(result, or(result, shl(and(b, value(0xFFL)), value(n * 7)))),
										set(pos, add(pos, value(n + 1)))),
								sequence(
										set(result, or(result, shl(and(b, value(0x7FL)), value(n * 7)))),
										readVarLongImpl(buf, pos, result, n + 1))) :
						sequence(
								set(result, or(result, shl(and(b, value(0xFFL)), value(n * 7)))),
								set(pos, add(pos, value(n + 1)))));
	}

	public static Expression readFloat(Expression buf, Variable pos) {
		return callStatic(Float.class, "intBitsToFloat", readInt(buf, pos));
	}

	public static Expression readDouble(Expression buf, Variable pos) {
		return callStatic(Double.class, "longBitsToDouble", readLong(buf, pos));
	}

	private static int varintSize(int value) {
		return 1 + (31 - Integer.numberOfLeadingZeros(value)) / 7;
	}

}
