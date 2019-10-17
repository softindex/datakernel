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
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;

import static io.datakernel.codegen.Expressions.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Provides methods for writing primitives
 * and Strings to byte arrays
 */
@SuppressWarnings({"unchecked"})
public final class SerializerExpressions {

	private static Class jdkUnsafe;
	private static int byteArrayBaseOffset;

	static {
		ClassLoader classLoader = ClassLoader.getSystemClassLoader();

		try {
			jdkUnsafe = classLoader.loadClass("jdk.internal.misc.Unsafe");
			Object unsafe = jdkUnsafe.getMethod("getUnsafe").invoke(null);
			byteArrayBaseOffset = (int) jdkUnsafe.getMethod("arrayBaseOffset", Class.class).invoke(unsafe, byte[].class);
			int byteArrayIndexScale = (int) jdkUnsafe.getMethod("arrayIndexScale", Class.class).invoke(unsafe, byte[].class);
			if (byteArrayIndexScale != 1) jdkUnsafe = null;
		} catch (Exception e) {
			jdkUnsafe = null;
		}
	}

	@NotNull
	private static Expression getUnsafe() {
		return callStatic(jdkUnsafe, "getUnsafe");
	}

	private static int varintSize(int value) {
		return 1 + (31 - Integer.numberOfLeadingZeros(value)) / 7;
	}

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
		return ensureRemaining(buf, pos, 2, sequence(
				jdkUnsafe != null ?
						call(getUnsafe(), "putShortUnaligned",
								cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class), cast(value, short.class), value(true)) :
						sequence(
								setArrayItem(buf, add(pos, value(0)), cast(ushr(value, value(8)), byte.class)),
								setArrayItem(buf, add(pos, value(1)), cast(ushr(value, value(0)), byte.class))),
				set(pos, add(pos, value(2)))));
	}

	public static Expression writeInt(Expression buf, Variable pos, Expression value) {
		return ensureRemaining(buf, pos, 4, sequence(
				jdkUnsafe != null ?
						call(getUnsafe(), "putIntUnaligned",
								cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class), value, value(true)) :
						sequence(
								setArrayItem(buf, add(pos, value(0)), cast(ushr(value, value(24)), byte.class)),
								setArrayItem(buf, add(pos, value(1)), cast(ushr(value, value(16)), byte.class)),
								setArrayItem(buf, add(pos, value(2)), cast(ushr(value, value(8)), byte.class)),
								setArrayItem(buf, add(pos, value(3)), cast(ushr(value, value(0)), byte.class))),
				set(pos, add(pos, value(4)))));
	}

	public static Expression writeLong(Expression buf, Variable pos, Expression value) {
		return ensureRemaining(buf, pos, 8, sequence(
				jdkUnsafe != null ?
						call(getUnsafe(), "putLongUnaligned",
								cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class), value, value(true)) :
						sequence(
								setArrayItem(buf, add(pos, value(0)), cast(ushr(value, value(56)), byte.class)),
								setArrayItem(buf, add(pos, value(1)), cast(ushr(value, value(48)), byte.class)),
								setArrayItem(buf, add(pos, value(2)), cast(ushr(value, value(40)), byte.class)),
								setArrayItem(buf, add(pos, value(3)), cast(ushr(value, value(32)), byte.class)),
								setArrayItem(buf, add(pos, value(4)), cast(ushr(value, value(24)), byte.class)),
								setArrayItem(buf, add(pos, value(5)), cast(ushr(value, value(16)), byte.class)),
								setArrayItem(buf, add(pos, value(6)), cast(ushr(value, value(8)), byte.class)),
								setArrayItem(buf, add(pos, value(7)), cast(ushr(value, value(0)), byte.class))),
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

	public static Expression writeUTF8mb3(Expression buf, Variable pos, Expression value) {
		return writeUTF8mb3Impl(buf, pos, value, false);
	}

	public static Expression writeUTF8mb3Nullable(Expression buf, Variable pos, Expression value) {
		return ifThenElse(isNull(value),
				writeByte(buf, pos, value((byte) 0)),
				writeUTF8mb3Impl(buf, pos, value, true));
	}

	public static Expression writeUTF8mb3Impl(Expression buf, Variable pos, Expression value, boolean nullable) {
		return let(
				call(value, "length"),
				length -> sequence(
						writeVarInt(buf, pos, nullable ? add(length, value(1)) : length),
						ensureRemaining(buf, pos, length, sequence(
								loop(value(0), length, i -> ifThenElse(cmpLe(
										cast(call(value, "charAt", i), byte.class), cast(value(0x007F), byte.class)),
										sequence(
												setArrayItem(buf, pos, cast(call(value, "charAt", i), byte.class)),
												set(pos, add(pos, value(1)))),
										sequence(
												writeUtfChar(buf, pos, cast(call(value, "charAt", i), byte.class)))))))));
	}

	private static Expression writeUtfChar(Expression buf, Variable pos, Expression value) {
		return ifThenElse(cmpLe(cast(value, byte.class), cast(value(0x007F), byte.class)),
				sequence(
						setArrayItem(buf, pos, cast(or(value(0xC0), and(value(0x1F), shr(value, value(6)))), byte.class)),
						setArrayItem(buf, add(pos, value(1)), cast(or(value(0x80), and(value(0x3F), value)), byte.class)),
						set(pos, add(pos, value(2)))
				),
				sequence(
						setArrayItem(buf, pos, cast(or(value(0xE0), and(value(0x0F), shr(value, value(12)))), byte.class)),
						setArrayItem(buf, add(pos, value(1)), cast(or(value(0x80), and(value(0x3F), shr(value, value(6)))), byte.class)),
						setArrayItem(buf, add(pos, value(2)), cast(or(value(0x80), and(value(0x3F), value)), byte.class)),
						set(pos, add(pos, value(3)))));
	}

	public static Expression writeUTF16(Expression buf, Variable pos, Expression value) {
		return writeUTF16Impl(buf, pos, value, false);
	}

	public static Expression writeUTF16Nullable(Expression buf, Variable pos, Expression value) {
		return ifThenElse(isNull(value),
				writeByte(buf, pos, value((byte) 0)),
				writeUTF16Impl(buf, pos, value, true));
	}

	public static Expression writeUTF16Impl(Expression buf, Variable pos, Expression value, boolean nullable) {
		return let(
				call(value, "length"),
				length -> sequence(
						writeVarInt(buf, pos, nullable ? add(length, value(1)) : length),
						ensureRemaining(buf, pos, length, sequence(
								loop(value(0), length, i -> sequence(
										setArrayItem(buf, pos, ushr(cast(call(value, "charAt", i), byte.class), value(8))),
										setArrayItem(buf, add(pos, value(1)), cast(call(value, "charAt", i), byte.class)),
										set(pos, add(pos, value(2)))))))));
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
								loop(value(0), length, i ->
										setArrayItem(buf, add(pos, i), cast(call(value, "charAt", i), byte.class))),
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
				jdkUnsafe != null ?
						call(getUnsafe(), "getShortUnaligned",
								cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class), value(true)) :
						or(shl(and(getArrayItem(buf, pos), value(0xFF)), value(8)), and(getArrayItem(buf, add(pos, value(1))), value(0xFF))),
				result -> sequence(
						set(pos, add(pos, value(2))),
						result));
	}

	public static Expression readInt(Expression buf, Variable pos) {
		return let(
				jdkUnsafe != null ?
						call(getUnsafe(), "getIntUnaligned",
								cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class), value(true)) :
						or(
								or(
										shl(and(getArrayItem(buf, add(pos, value(0))), value(0xFF)), value(24)),
										shl(and(getArrayItem(buf, add(pos, value(1))), value(0xFF)), value(16))),
								or(
										shl(and(getArrayItem(buf, add(pos, value(2))), value(0xFF)), value(8)),
										shl(and(getArrayItem(buf, add(pos, value(3))), value(0xFF)), value(0)))),
				result -> sequence(
						set(pos, add(pos, value(4))),
						result));
	}

	public static Expression readLong(Expression buf, Variable pos) {
		return let(
				jdkUnsafe != null ?
						call(getUnsafe(), "getLongUnaligned",
								cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class), value(true)) :
						or(
								or(
										or(
												shl(and(getArrayItem(buf, add(pos, value(0))), value(0xFFL)), value(56)),
												shl(and(getArrayItem(buf, add(pos, value(1))), value(0xFFL)), value(48))),
										or(
												shl(and(getArrayItem(buf, add(pos, value(2))), value(0xFFL)), value(40)),
												shl(and(getArrayItem(buf, add(pos, value(3))), value(0xFFL)), value(32)))),
								or(
										or(
												shl(and(getArrayItem(buf, add(pos, value(4))), value(0xFFL)), value(24)),
												shl(and(getArrayItem(buf, add(pos, value(5))), value(0xFFL)), value(16))),
										or(
												shl(and(getArrayItem(buf, add(pos, value(6))), value(0xFFL)), value(8)),
												shl(and(getArrayItem(buf, add(pos, value(7))), value(0xFFL)), value(0))))),
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

	public static Expression readUTF8(Expression buf, Variable pos) {
		return let(readVarInt(buf, pos), len -> readUTF8Impl(buf, pos, len));
	}

	public static Expression readUTF8Nullable(Expression buf, Variable pos) {
		return let(readVarInt(buf, pos), len ->
				ifThenElse(cmpEq(len, value(0)),
						nullRef(String.class),
						readUTF8Impl(buf, pos, dec(len))));
	}

	private static Expression readUTF8Impl(Expression buf, Variable pos, Expression len) {
		return ifThenElse(cmpEq(len, value(0)),
				value(""),
				sequence(
						ensureRemaining(buf, pos, len,
								constructor(String.class, buf, pos, len, value(UTF_8, Charset.class))),
						set(pos, add(pos, len))));
	}

	public static Expression readUTF8mb3(Expression buf, Variable pos) {
		return let(readVarInt(buf, pos), len -> readUTF8mb3Impl(buf, pos, len));
	}

	public static Expression readUTF8mb3Nullable(Expression buf, Variable pos) {
		return let(readVarInt(buf, pos), len ->
				ifThenElse(cmpEq(len, value(0)),
						nullRef(String.class),
						readUTF8mb3Impl(buf, pos, dec(len))));
	}

	private static Expression readUTF8mb3Impl(Expression buf, Variable pos, Expression len) {
		return ifThenElse(cmpEq(len, value(0)),
				value(""),
				ensureRemaining(buf, pos, len, constructUTF8mb3String(buf, pos, len)));
	}

	private static Expression constructUTF8mb3String(Expression buf, Variable pos, Expression len) {
		return constructor(String.class, let(newArray(char[].class, len), arr ->
						sequence(loop(value(0), len, i ->
										setArrayItem(arr, i, readUtfChar(buf, pos))),
								arr)),
				value(0), len);
	}

	private static Expression readUtfChar(Expression buf, Variable pos) {
		return let(and(readByte(buf, pos), value(0xFF)), ch ->
				ifThenElse(cmpGe(ch, value(0x80)),
						ifThenElse(cmpLt(ch, value(0xE0)),
								or(shl(and(ch, value(0x1F)), value(6)), and(readByte(buf, pos), value(0x3F))),
								or(shl(and(ch, value(0x0F)), value(12)),
										or(shl(and(ch, value(0x3F)), value(6)), and(readByte(buf, pos), value(0x3F))))),
						ch));
	}

	public static Expression readUTF16(Expression buf, Variable pos) {
		return let(readVarInt(buf, pos), len -> readUTF16Impl(buf, pos, len));
	}

	public static Expression readUTF16Nullable(Expression buf, Variable pos) {
		return let(readVarInt(buf, pos), len ->
				ifThenElse(cmpEq(len, value(0)),
						nullRef(String.class),
						readUTF16Impl(buf, pos, dec(len))));
	}

	private static Expression readUTF16Impl(Expression buf, Variable pos, Expression len) {
		return ifThenElse(cmpEq(len, value(0)),
				value(""),
				ensureRemaining(buf, pos, len, constructUTF16String(buf, pos, len)));
	}

	@NotNull
	private static Expression constructUTF16String(Expression buf, Variable pos, Expression len) {
		return constructor(String.class, let(newArray(char[].class, len), arr ->
						sequence(loop(value(0), len, i ->
										setArrayItem(arr, i, let(add(pos, mul(i, value(2))), newPos -> readChar(buf, newPos)))),
								set(pos, add(pos, mul(len, value(2)))),
								arr)),
				value(0), len);
	}

	public static Expression readIso88591(Expression buf, Variable pos) {
		return let(readVarInt(buf, pos), len -> readIso88591Impl(buf, pos, len));
	}

	public static Expression readIso88591Nullable(Expression buf, Variable pos) {
		return let(readVarInt(buf, pos), len ->
				ifThenElse(cmpEq(len, value(0)),
						nullRef(String.class),
						readIso88591Impl(buf, pos, dec(len))));
	}

	private static Expression readIso88591Impl(Expression buf, Variable pos, Expression len) {
		return ifThenElse(cmpEq(len, value(0)),
				value(""),
				ensureRemaining(buf, pos, len, constructIso88591String(buf, pos, len)));
	}

	private static Expression constructIso88591String(Expression buf, Variable pos, Expression len) {
		return constructor(String.class, let(newArray(char[].class, len), arr ->
						sequence(loop(value(0), len, i ->
										setArrayItem(arr, i, cast(and(readByte(buf, pos), value(0xFF)), char.class))),
								arr)),
				value(0), len);
	}
}
