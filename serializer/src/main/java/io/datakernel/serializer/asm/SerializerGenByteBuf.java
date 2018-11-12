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

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.SerializationUtils;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.SerializerBuilder;

import static io.datakernel.codegen.Expressions.*;

public class SerializerGenByteBuf implements SerializerGen, NullableOptimization {
	private final boolean writeWithRecycle;
	private final boolean readWithSlice;
	private final boolean nullable;

	public SerializerGenByteBuf(boolean writeWithRecycle, boolean readWithSlice) {
		this.writeWithRecycle = writeWithRecycle;
		this.readWithSlice = readWithSlice;
		this.nullable = false;
	}

	SerializerGenByteBuf(boolean writeWithRecycle, boolean readWithSlice, boolean nullable) {
		this.writeWithRecycle = writeWithRecycle;
		this.readWithSlice = readWithSlice;
		this.nullable = nullable;
	}

	@Override
	public void getVersions(VersionsCollector versions) {
	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return ByteBuf.class;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
	}

	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		return callStatic(SerializerGenByteBuf.class,
				"write" + (writeWithRecycle ? "Recycle" : "") + (nullable ? "Nullable" : ""),
				byteArray, off, cast(value, ByteBuf.class));
	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		return callStatic(SerializerGenByteBuf.class,
				"read" + (readWithSlice ? "Slice" : "") + (nullable ? "Nullable" : ""),
				arg(0));
	}

	public static int write(byte[] output, int offset, ByteBuf buf) {
		offset = SerializationUtils.writeVarInt(output, offset, buf.readRemaining());
		offset = SerializationUtils.write(output, offset, buf.array(), buf.readPosition(), buf.readRemaining());
		return offset;
	}

	public static int writeNullable(byte[] output, int offset, ByteBuf buf) {
		if (buf == null) {
			output[offset] = 0;
			return offset + 1;
		} else {
			offset = SerializationUtils.writeVarInt(output, offset, buf.readRemaining() + 1);
			offset = SerializationUtils.write(output, offset, buf.array(), buf.readPosition(), buf.readRemaining());
			return offset;
		}
	}

	public static int writeRecycle(byte[] output, int offset, ByteBuf buf) {
		offset = SerializationUtils.writeVarInt(output, offset, buf.readRemaining());
		offset = SerializationUtils.write(output, offset, buf.array(), buf.readPosition(), buf.readRemaining());
		buf.recycle();
		return offset;
	}

	public static int writeRecycleNullable(byte[] output, int offset, ByteBuf buf) {
		if (buf == null) {
			output[offset] = 0;
			return offset + 1;
		} else {
			offset = SerializationUtils.writeVarInt(output, offset, buf.readRemaining() + 1);
			offset = SerializationUtils.write(output, offset, buf.array(), buf.readPosition(), buf.readRemaining());
			buf.recycle();
			return offset;
		}
	}

	public static ByteBuf read(ByteBuf input) {
		int length = input.readVarInt();
		ByteBuf byteBuf = ByteBufPool.allocate(length);
		input.read(byteBuf.array(), 0, length);
		byteBuf.writePosition(length);
		return byteBuf;
	}

	@Nullable
	public static ByteBuf readNullable(ByteBuf input) {
		int length = input.readVarInt();
		if (length == 0) return null;
		length--;
		ByteBuf byteBuf = ByteBufPool.allocate(length);
		input.read(byteBuf.array(), 0, length);
		byteBuf.writePosition(length);
		return byteBuf;
	}

	public static ByteBuf readSlice(ByteBuf input) {
		int length = input.readVarInt();
		ByteBuf result = input.slice(length);
		input.moveReadPosition(length);
		return result;
	}

	@Nullable
	public static ByteBuf readSliceNullable(ByteBuf input) {
		int length = input.readVarInt();
		if (length == 0) return null;
		length--;
		ByteBuf result = input.slice(length);
		input.moveReadPosition(length);
		return result;
	}

	@Override
	public SerializerGen asNullable() {
		return new SerializerGenByteBuf(writeWithRecycle, readWithSlice, true);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenByteBuf that = (SerializerGenByteBuf) o;

		if (readWithSlice != that.readWithSlice) return false;
		return nullable == that.nullable;

	}

	@Override
	public int hashCode() {
		int result = readWithSlice ? 1 : 0;
		result = 31 * result + (nullable ? 1 : 0);
		return result;
	}
}
