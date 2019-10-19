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

	private static Expression get(Expression buf, Expression pos) {
		return jdkUnsafe != null ?
				call(getUnsafe(), "getByteUnaligned",
						cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class)) :
				getArrayItem(buf, pos);
	}

	private static Expression put(Expression buf, Expression pos, Expression value) {
		return jdkUnsafe != null ?
				call(getUnsafe(), "putByteUnaligned",
						cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class), cast(value, byte.class)) :
				setArrayItem(buf, pos, cast(value, byte.class));
	}

	public static Expression writeBytes(Expression buf, Variable pos, Expression bytes) {
		return writeBytes(buf, pos, bytes, value(0), length(bytes));
	}

	public static Expression writeBytes(Expression buf, Variable pos, Expression bytes, Expression bytesOff, Expression bytesLen) {
		return ensureRemaining(buf, pos, bytesLen, sequence(
				callStatic(System.class, "arraycopy", bytes, bytesOff, buf, pos, bytesLen),
				set(pos, add(pos, bytesLen))));
	}

	public static Expression writeByte(Expression buf, Variable pos, Expression value) {
		return ensureRemaining(buf, pos, 1, sequence(
				setArrayItem(buf, pos, value),
				set(pos, add(pos, value(1)))));
	}

	public static Expression writeBoolean(Expression buf, Variable pos, Expression value) {
		return writeByte(buf, pos, value);
	}

	public static Expression writeChar(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return writeShort(buf, pos, value, bigEndian);
	}

	public static Expression writeShort(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return ensureRemaining(buf, pos, 2, sequence(
				jdkUnsafe != null ?
						call(getUnsafe(), "putShortUnaligned",
								cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class), cast(value, short.class), value(bigEndian)) :
						sequence(
								setArrayItem(buf, add(pos, value(0)), cast(ushr(value, value(bigEndian ? 8 : 0)), byte.class)),
								setArrayItem(buf, add(pos, value(1)), cast(ushr(value, value(bigEndian ? 0 : 8)), byte.class))),
				set(pos, add(pos, value(2)))));
	}

	public static Expression writeInt(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return ensureRemaining(buf, pos, 4, sequence(
				jdkUnsafe != null ?
						call(getUnsafe(), "putIntUnaligned",
								cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class), value, value(bigEndian)) :
						sequence(
								setArrayItem(buf, add(pos, value(0)), cast(ushr(value, value(bigEndian ? 24 : 0)), byte.class)),
								setArrayItem(buf, add(pos, value(1)), cast(ushr(value, value(bigEndian ? 16 : 8)), byte.class)),
								setArrayItem(buf, add(pos, value(2)), cast(ushr(value, value(bigEndian ? 8 : 16)), byte.class)),
								setArrayItem(buf, add(pos, value(3)), cast(ushr(value, value(bigEndian ? 0 : 24)), byte.class))),
				set(pos, add(pos, value(4)))));
	}

	public static Expression writeLong(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return ensureRemaining(buf, pos, 8, sequence(
				jdkUnsafe != null ?
						call(getUnsafe(), "putLongUnaligned",
								cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class), value, value(bigEndian)) :
						sequence(
								setArrayItem(buf, add(pos, value(0)), cast(ushr(value, value(bigEndian ? 56 : 0)), byte.class)),
								setArrayItem(buf, add(pos, value(1)), cast(ushr(value, value(bigEndian ? 48 : 8)), byte.class)),
								setArrayItem(buf, add(pos, value(2)), cast(ushr(value, value(bigEndian ? 40 : 16)), byte.class)),
								setArrayItem(buf, add(pos, value(3)), cast(ushr(value, value(bigEndian ? 32 : 24)), byte.class)),
								setArrayItem(buf, add(pos, value(4)), cast(ushr(value, value(bigEndian ? 24 : 32)), byte.class)),
								setArrayItem(buf, add(pos, value(5)), cast(ushr(value, value(bigEndian ? 16 : 40)), byte.class)),
								setArrayItem(buf, add(pos, value(6)), cast(ushr(value, value(bigEndian ? 8 : 48)), byte.class)),
								setArrayItem(buf, add(pos, value(7)), cast(ushr(value, value(bigEndian ? 0 : 56)), byte.class))),
				set(pos, add(pos, value(8)))));
	}

	public static Expression writeVarInt(Expression buf, Variable pos, Expression value) {
		return writeVarInt(buf, pos, value, 5);
	}

	public static Expression writeVarInt(Expression buf, Variable pos, Expression value, int bytes) {
		return ensureRemaining(buf, pos, 5,
				let(value, v -> writeVarIntImpl(buf, pos, v, 0, bytes)));
	}

	private static Expression writeVarIntImpl(Expression buf, Variable pos, Variable v, int n, int bytes) {
		return n != (bytes - 1) ?
				ifThenElse(
						cmpEq(and(v, value(~0x7F)), value(0)),
						sequence(
								setArrayItem(buf, add(pos, value(n)), cast(v, byte.class)),
								set(pos, add(pos, value(n + 1)))),
						sequence(
								setArrayItem(buf, add(pos, value(n)), cast(or(v, value(0x80)), byte.class)),
								set(v, ushr(v, value(7))),
								writeVarIntImpl(buf, pos, v, n + 1, bytes)
						)
				) :
				sequence(
						setArrayItem(buf, add(pos, value(n)), cast(v, byte.class)),
						set(pos, add(pos, value(n + 1))));
	}

	public static Expression writeVarLong(Expression buf, Variable pos, Expression value, int bytes) {
		return ensureRemaining(buf, pos, bytes,
				let(value, v -> writeVarLongImpl(buf, pos, v, 0, bytes)));
	}

	private static Expression writeVarLongImpl(Expression buf, Variable pos, Variable v, int n, int bytes) {
		return n != (bytes - 1) ?
				ifThenElse(
						cmpEq(and(v, value(~0x7FL)), value(0L)),
						sequence(
								setArrayItem(buf, add(pos, value(n)), cast(v, byte.class)),
								set(pos, add(pos, value(n + 1)))),
						sequence(
								setArrayItem(buf, add(pos, value(n)), cast(or(v, value(0x80L)), byte.class)),
								set(v, ushr(v, value(7))),
								writeVarLongImpl(buf, pos, v, n + 1, bytes)
						)
				) :
				sequence(
						setArrayItem(buf, add(pos, value(n)), cast(v, byte.class)),
						set(pos, add(pos, value(n + 1))));
	}

	public static Expression writeFloat(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return writeInt(buf, pos, callStatic(Float.class, "floatToIntBits", cast(value, float.class)), bigEndian);
	}

	public static Expression writeDouble(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return writeLong(buf, pos, callStatic(Double.class, "doubleToLongBits", cast(value, double.class)), bigEndian);
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
						ensureRemaining(buf, pos, mul(length, value(3)), sequence(
								loop(value(0), length,
										i -> let(call(value, "charAt", i),
												c -> writeUtfMb3Char(buf, pos, cast(c, int.class))))))));
	}

	@NotNull
	private static Expression writeUtfMb3Char(Expression buf, Variable pos, Expression c) {
		return ifThenElse(cmpLe(c, value(0x007F)),
				sequence(
						put(buf, pos, cast(c, byte.class)),
						set(pos, add(pos, value(1)))),
				ifThenElse(cmpLe(c, value(0x07FF)),
						sequence(
								put(buf, pos, cast(or(value(0xC0), and(shr(c, value(6)), value(0x1F))), byte.class)),
								put(buf, add(pos, value(1)), cast(or(value(0x80), and(c, value(0x3F))), byte.class)),
								set(pos, add(pos, value(2)))),
						sequence(
								put(buf, pos, cast(or(value(0xE0), and(shr(c, value(12)), value(0x0F))), byte.class)),
								put(buf, add(pos, value(1)), cast(or(value(0x80), and(shr(c, value(6)), value(0x3F))), byte.class)),
								put(buf, add(pos, value(2)), cast(or(value(0x80), and(c, value(0x3F))), byte.class)),
								set(pos, add(pos, value(3))))
				)
		);
	}

	public static Expression writeUTF16(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return writeUTF16Impl(buf, pos, value, false, bigEndian);
	}

	public static Expression writeUTF16Nullable(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return ifThenElse(isNull(value),
				writeByte(buf, pos, value((byte) 0)),
				writeUTF16Impl(buf, pos, value, true, bigEndian));
	}

	public static Expression writeUTF16Impl(Expression buf, Variable pos, Expression value, boolean nullable, boolean bigEndian) {
		return let(
				call(value, "length"),
				length -> sequence(
						writeVarInt(buf, pos, nullable ? add(length, value(1)) : length),
						ensureRemaining(buf, pos, mul(length, value(2)), sequence(
								loop(value(0), length,
										i -> let(add(pos, mul(i, value(2))),
												p -> writeChar(buf, p, call(value, "charAt", i), bigEndian))),
								set(pos, add(pos, mul(length, value(2))))))));
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
										i -> put(buf, add(pos, i), cast(call(value, "charAt", i), byte.class))),
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
		return let(get(buf, pos),
				b -> sequence(
						set(pos, inc(pos)),
						b));
	}

	public static Expression readBoolean(Expression buf, Variable pos) {
		return cast(readByte(buf, pos), boolean.class);
	}

	public static Expression readChar(Expression buf, Variable pos, boolean bigEndian) {
		return cast(readShort(buf, pos, bigEndian), char.class);
	}

	public static Expression readShort(Expression buf, Variable pos, boolean bigEndian) {
		return let(
				jdkUnsafe != null ?
						call(getUnsafe(), "getShortUnaligned",
								cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class), value(bigEndian)) :
						or(
								shl(and(getArrayItem(buf, add(pos, value(0))), value(0xFF)), value(bigEndian ? 8 : 0)),
								shl(and(getArrayItem(buf, add(pos, value(1))), value(0xFF)), value(bigEndian ? 0 : 8))),
				result -> sequence(
						set(pos, add(pos, value(2))),
						result));
	}

	public static Expression readInt(Expression buf, Variable pos, boolean bigEndian) {
		return let(
				jdkUnsafe != null ?
						call(getUnsafe(), "getIntUnaligned",
								cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class), value(bigEndian)) :
						or(
								or(
										shl(and(getArrayItem(buf, add(pos, value(0))), value(0xFF)), value(bigEndian ? 24 : 0)),
										shl(and(getArrayItem(buf, add(pos, value(1))), value(0xFF)), value(bigEndian ? 16 : 8))),
								or(
										shl(and(getArrayItem(buf, add(pos, value(2))), value(0xFF)), value(bigEndian ? 8 : 16)),
										shl(and(getArrayItem(buf, add(pos, value(3))), value(0xFF)), value(bigEndian ? 0 : 24)))),
				result -> sequence(
						set(pos, add(pos, value(4))),
						result));
	}

	public static Expression readLong(Expression buf, Variable pos, boolean bigEndian) {
		return let(
				jdkUnsafe != null ?
						call(getUnsafe(), "getLongUnaligned",
								cast(buf, Object.class), cast(add(value(byteArrayBaseOffset), pos), long.class), value(bigEndian)) :
						or(
								or(
										or(
												shl(and(getArrayItem(buf, add(pos, value(0))), value(0xFFL)), value(bigEndian ? 56 : 0)),
												shl(and(getArrayItem(buf, add(pos, value(1))), value(0xFFL)), value(bigEndian ? 48 : 8))),
										or(
												shl(and(getArrayItem(buf, add(pos, value(2))), value(0xFFL)), value(bigEndian ? 40 : 16)),
												shl(and(getArrayItem(buf, add(pos, value(3))), value(0xFFL)), value(bigEndian ? 32 : 24)))),
								or(
										or(
												shl(and(getArrayItem(buf, add(pos, value(4))), value(0xFFL)), value(bigEndian ? 24 : 32)),
												shl(and(getArrayItem(buf, add(pos, value(5))), value(0xFFL)), value(bigEndian ? 16 : 40))),
										or(
												shl(and(getArrayItem(buf, add(pos, value(6))), value(0xFFL)), value(bigEndian ? 8 : 48)),
												shl(and(getArrayItem(buf, add(pos, value(7))), value(0xFFL)), value(bigEndian ? 0 : 56))))),
				result -> sequence(
						set(pos, add(pos, value(8))),
						result));
	}

	public static Expression readVarInt(Expression buf, Variable pos) {
		return readVarInt(buf, pos, 5, false);
	}

	public static Expression readVarInt(Expression buf, Variable pos, int bytes, boolean checkLastByte) {
		return let(value(0), result -> sequence(readVarIntImpl(buf, pos, result, 0, bytes, checkLastByte), result));
	}

	private static Expression readVarIntImpl(Expression buf, Variable pos, Variable result, int n, int bytes, boolean checkLastByte) {
		return let(getArrayItem(buf, add(pos, value(n))),
				b -> n < (bytes - 1) ?
						ifThenElse(cmpGe(b, value((byte) 0)),
								sequence(
										set(result, or(result, shl(cast(b, int.class), value(n * 7)))),
										set(pos, add(pos, value(n + 1)))),
								sequence(
										set(result, or(result, shl(and(b, value(0x7F)), value(n * 7)))),
										readVarIntImpl(buf, pos, result, n + 1, bytes, checkLastByte))) :
						!checkLastByte ?
								sequence(
										set(result, or(result, shl(cast(b, int.class), value(n * 7)))),
										set(pos, add(pos, value(n + 1)))) :
								ifThenElse(cmpEq(and(b, value(-1 << (n * 7))), value(0)),
										sequence(
												set(result, or(result, shl(cast(b, int.class), value(n * 7)))),
												set(pos, add(pos, value(n + 1)))),
										exception(IllegalArgumentException.class))
		);
	}

	public static Expression readVarLong(Expression buf, Variable pos, int bytes, boolean checkLastByte) {
		return let(value(0L), result -> sequence(readVarLongImpl(buf, pos, result, 0, bytes, checkLastByte), result));
	}

	private static Expression readVarLongImpl(Expression buf, Variable pos, Variable result, int n, int bytes, boolean checkLastByte) {
		return let(getArrayItem(buf, add(pos, value(n))),
				b -> n < (bytes - 1) ?
						ifThenElse(cmpGe(b, value((byte) 0)),
								sequence(
										set(result, or(result, shl(cast(b, long.class), value(n * 7)))),
										set(pos, add(pos, value(n + 1)))),
								sequence(
										set(result, or(result, shl(and(b, value(0x7FL)), value(n * 7)))),
										readVarLongImpl(buf, pos, result, n + 1, bytes, checkLastByte))) :
						!checkLastByte ?
								sequence(
										set(result, or(result, shl(cast(b, long.class), value(n * 7)))),
										set(pos, add(pos, value(n + 1)))) :
								ifThenElse(cmpEq(and(b, value(-1L << (n * 7))), value(0L)),
										sequence(
												set(result, or(result, shl(cast(b, long.class), value(n * 7)))),
												set(pos, add(pos, value(n + 1)))),
										exception(IllegalArgumentException.class))
		);
	}

	public static Expression readFloat(Expression buf, Variable pos, boolean bigEndian) {
		return callStatic(Float.class, "intBitsToFloat", readInt(buf, pos, bigEndian));
	}

	public static Expression readDouble(Expression buf, Variable pos, boolean bigEndian) {
		return callStatic(Double.class, "longBitsToDouble", readLong(buf, pos, bigEndian));
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
				constructor(String.class, let(newArray(char[].class, len),
						arr -> sequence(loop(value(0), len,
								i -> setArrayItem(arr, i, readUtf8mb3Char(buf, pos))),
								arr)),
						value(0), len));
	}

	private static Expression readUtf8mb3Char(Expression buf, Variable pos) {
		return let(and(readByte(buf, pos), value(0xFF)), c ->
				ifThenElse(cmpLt(c, value(0x80)),
						c,
						ifThenElse(cmpLt(c, value(0xE0)),
								or(
										shl(and(c, value(0x1F)), value(6)),
										and(readByte(buf, pos), value(0x3F))),
								or(
										shl(and(c, value(0x0F)), value(12)),
										or(
												shl(and(readByte(buf, pos), value(0x3F)), value(6)),
												and(readByte(buf, pos), value(0x3F)))))));
	}

	public static Expression readUTF16(Expression buf, Variable pos, boolean bigEndian) {
		return let(readVarInt(buf, pos), len -> readUTF16Impl(buf, pos, len, bigEndian));
	}

	public static Expression readUTF16Nullable(Expression buf, Variable pos, boolean bigEndian) {
		return let(readVarInt(buf, pos), len ->
				ifThenElse(cmpEq(len, value(0)),
						nullRef(String.class),
						readUTF16Impl(buf, pos, dec(len), bigEndian)));
	}

	private static Expression readUTF16Impl(Expression buf, Variable pos, Expression len, boolean bigEndian) {
		return ifThenElse(cmpEq(len, value(0)),
				value(""),
				ensureRemaining(buf, pos, mul(len, value(2)),
						constructor(String.class, let(newArray(char[].class, len),
								arr -> sequence(loop(value(0), len,
										i -> setArrayItem(arr, i,
												let(add(pos, mul(i, value(2))), p -> readChar(buf, p, bigEndian)))),
										set(pos, add(pos, mul(len, value(2)))),
										arr)),
								value(0), len)));
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
				ensureRemaining(buf, pos, len,
						constructor(String.class, let(newArray(char[].class, len),
								arr -> sequence(loop(value(0), len,
										i -> setArrayItem(arr, i, cast(and(get(buf, add(pos, i)), value(0xFF)), char.class))),
										set(pos, add(pos, len)),
										arr)),
								value(0), len)));
	}

}
