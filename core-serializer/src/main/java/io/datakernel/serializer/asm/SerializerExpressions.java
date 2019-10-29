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
import io.datakernel.serializer.BinaryOutputUtils;
import org.jetbrains.annotations.NotNull;

import static io.datakernel.codegen.Expressions.*;

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

	public static Expression writeBytes(Expression buf, Variable pos, Expression bytes) {
		return writeBytes(buf, pos, bytes, value(0), length(bytes));
	}

	public static Expression writeBytes(Expression buf, Variable pos, Expression bytes, Expression bytesOff, Expression bytesLen) {
		return sequence(
				callStatic(System.class, "arraycopy", bytes, bytesOff, buf, pos, bytesLen),
				set(pos, add(pos, bytesLen)));
	}

	public static Expression writeByte(Expression buf, Variable pos, Expression value) {
		return sequence(
				setArrayItem(buf, pos, value),
				set(pos, add(pos, value(1))));
	}

	public static Expression writeBoolean(Expression buf, Variable pos, Expression value) {
		return writeByte(buf, pos, value);
	}

	public static Expression writeShort(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return jdkUnsafe != null ?
				ensureRemaining(buf, pos, 2, sequence(
						call(getUnsafe(), "putShortUnaligned",
								buf, cast(add(value(byteArrayBaseOffset), pos), long.class), value, value(bigEndian)),
						set(pos, add(pos, value(2)))
				)) :
				set(pos, callStatic(BinaryOutputUtils.class, "writeShort" + (bigEndian ? "" : "LE"), buf, pos, cast(value, short.class)));
	}

	public static Expression writeChar(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return jdkUnsafe != null ?
				ensureRemaining(buf, pos, 2, sequence(
						call(getUnsafe(), "putCharUnaligned",
								buf, cast(add(value(byteArrayBaseOffset), pos), long.class), value, value(bigEndian)),
						set(pos, add(pos, value(2)))
				)) :
				set(pos, callStatic(BinaryOutputUtils.class, "writeChar" + (bigEndian ? "" : "LE"), buf, pos, cast(value, char.class)));
	}

	public static Expression writeInt(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return jdkUnsafe != null ?
				ensureRemaining(buf, pos, 4, sequence(
						call(getUnsafe(), "putIntUnaligned",
								buf, cast(add(value(byteArrayBaseOffset), pos), long.class), value, value(bigEndian)),
						set(pos, add(pos, value(4)))
				)) :
				set(pos, callStatic(BinaryOutputUtils.class, "writeInt" + (bigEndian ? "" : "LE"), buf, pos, cast(value, int.class)));
	}

	public static Expression writeLong(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return jdkUnsafe != null ?
				ensureRemaining(buf, pos, 8, sequence(
						call(getUnsafe(), "putLongUnaligned",
								buf, cast(add(value(byteArrayBaseOffset), pos), long.class), value, value(bigEndian)),
						set(pos, add(pos, value(8)))
				)) :
				set(pos, callStatic(BinaryOutputUtils.class, "writeLong" + (bigEndian ? "" : "LE"), buf, pos, cast(value, long.class)));
	}

	public static Expression writeVarInt(Expression buf, Variable pos, Expression value) {
		return set(pos,
				callStatic(BinaryOutputUtils.class, "writeVarInt", buf, pos, cast(value, int.class)));
	}

	public static Expression writeVarLong(Expression buf, Variable pos, Expression value) {
		return set(pos,
				callStatic(BinaryOutputUtils.class, "writeVarLong", buf, pos, cast(value, long.class)));
	}

	public static Expression writeFloat(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return writeInt(buf, pos, callStatic(Float.class, "floatToIntBits", cast(value, float.class)), bigEndian);
	}

	public static Expression writeDouble(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return writeLong(buf, pos, callStatic(Double.class, "doubleToLongBits", cast(value, double.class)), bigEndian);
	}

	public static Expression ensureRemaining(Expression buf, Variable pos, int size, Expression next) {
		return ensureRemaining(buf, pos, value(size), next);
	}

	public static Expression ensureRemaining(Expression buf, Variable pos, Expression size, Expression next) {
		return ifThenElse(cmpGt(add(pos, size), length(buf)),
				exception(ArrayIndexOutOfBoundsException.class),
				next);
	}

	public static Expression array(Expression in) {
		return call(in, "array");
	}

	public static Expression pos(Expression in) {
		return call(in, "pos");
	}

	public static Expression pos(Expression in, Expression newPos) {
		return call(in, "pos", newPos);
	}

	public static Expression move(Expression in, Expression bytes) {
		return call(in, "move", bytes);
	}

	private static Expression move(Expression in, int bytes) {
		return call(in, "move", value(bytes));
	}

	public static Expression readBytes(Expression in, Expression buf) {
		return call(in, "read", buf);
	}

	public static Expression readBytes(Expression in, Expression buf, Expression off, Expression len) {
		return call(in, "read", buf, off, len);
	}

	public static Expression readByte(Expression in) {
		return call(in, "readByte");
	}

	public static Expression readBoolean(Expression in) {
		return call(in, "readBoolean");
	}

	public static Expression readShort(Expression in, boolean bigEndian) {
		return jdkUnsafe != null ?
				let(call(getUnsafe(), "getShortUnaligned",
						array(in), cast(add(value(byteArrayBaseOffset), pos(in)), long.class), value(bigEndian)),
						result -> sequence(
								move(in, 2),
								result)
				) :
				call(in, "readShort" + (bigEndian ? "" : "LE"));
	}

	public static Expression readChar(Expression in, boolean bigEndian) {
		return jdkUnsafe != null ?
				let(call(getUnsafe(), "getCharUnaligned",
						array(in), cast(add(value(byteArrayBaseOffset), pos(in)), long.class), value(bigEndian)),
						result -> sequence(
								move(in, 2),
								result)
				) :
				call(in, "readChar" + (bigEndian ? "" : "LE"));
	}

	public static Expression readInt(Expression in, boolean bigEndian) {
		return jdkUnsafe != null ?
				let(call(getUnsafe(), "getIntUnaligned",
						array(in), cast(add(value(byteArrayBaseOffset), pos(in)), long.class), value(bigEndian)),
						result -> sequence(
								move(in, 4),
								result)
				) :
				call(in, "readInt" + (bigEndian ? "" : "LE"));
	}

	public static Expression readLong(Expression in, boolean bigEndian) {
		return jdkUnsafe != null ?
				let(call(getUnsafe(), "getLongUnaligned",
						array(in), cast(add(value(byteArrayBaseOffset), pos(in)), long.class), value(bigEndian)),
						result -> sequence(
								move(in, 8),
								result)
				) :
				call(in, "readLong" + (bigEndian ? "" : "LE"));
	}

	public static Expression readVarInt(Expression in) {
		return call(in, "readVarInt");
	}

	public static Expression readVarLong(Expression in) {
		return call(in, "readVarLong");
	}

	public static Expression readFloat(Expression in, boolean bigEndian) {
		return callStatic(Float.class, "intBitsToFloat", readInt(in, bigEndian));
	}

	public static Expression readDouble(Expression in, boolean bigEndian) {
		return callStatic(Double.class, "longBitsToDouble", readLong(in, bigEndian));
	}

}
