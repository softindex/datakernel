package io.datakernel.memcache.protocol;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.HasNullable;
import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.util.BinaryInput;
import io.datakernel.serializer.util.BinaryOutputUtils;

import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static java.util.Collections.emptySet;

@SuppressWarnings("unused")
public class SerializerGenByteBuf implements SerializerGen, HasNullable {
	private final boolean writeWithRecycle;
	private final boolean wrap;
	private final boolean nullable;

	public SerializerGenByteBuf(boolean writeWithRecycle, boolean wrap) {
		this(writeWithRecycle, wrap, false);
	}

	private SerializerGenByteBuf(boolean writeWithRecycle, boolean wrap, boolean nullable) {
		this.writeWithRecycle = writeWithRecycle;
		this.wrap = wrap;
		this.nullable = nullable;
	}

	@Override
	public SerializerGen withNullable() {
		return new SerializerGenByteBuf(writeWithRecycle, wrap, true);
	}

	@Override
	public void accept(Visitor visitor) {
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
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
	public Expression serialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return set(off,
				callStatic(SerializerGenByteBuf.class,
						"write" + (writeWithRecycle ? "Recycle" : "") + (nullable ? "Nullable" : ""),
						byteArray, off, cast(value, ByteBuf.class)));
	}

	@Override
	public Expression deserialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		return callStatic(SerializerGenByteBuf.class,
				"read" + (wrap ? "Slice" : "") + (nullable ? "Nullable" : ""),
				arg(0));
	}

	public static int write(byte[] output, int offset, ByteBuf buf) {
		offset = BinaryOutputUtils.writeVarInt(output, offset, buf.readRemaining());
		offset = BinaryOutputUtils.write(output, offset, buf.array(), buf.head(), buf.readRemaining());
		return offset;
	}

	public static int writeNullable(byte[] output, int offset, ByteBuf buf) {
		if (buf == null) {
			output[offset] = 0;
			return offset + 1;
		} else {
			offset = BinaryOutputUtils.writeVarInt(output, offset, buf.readRemaining() + 1);
			offset = BinaryOutputUtils.write(output, offset, buf.array(), buf.head(), buf.readRemaining());
			return offset;
		}
	}

	public static int writeRecycle(byte[] output, int offset, ByteBuf buf) {
		offset = BinaryOutputUtils.writeVarInt(output, offset, buf.readRemaining());
		offset = BinaryOutputUtils.write(output, offset, buf.array(), buf.head(), buf.readRemaining());
		buf.recycle();
		return offset;
	}

	public static int writeRecycleNullable(byte[] output, int offset, ByteBuf buf) {
		if (buf == null) {
			output[offset] = 0;
			return offset + 1;
		} else {
			offset = BinaryOutputUtils.writeVarInt(output, offset, buf.readRemaining() + 1);
			offset = BinaryOutputUtils.write(output, offset, buf.array(), buf.head(), buf.readRemaining());
			buf.recycle();
			return offset;
		}
	}

	public static ByteBuf read(BinaryInput input) {
		int length = input.readVarInt();
		ByteBuf byteBuf = ByteBufPool.allocate(length);
		input.read(byteBuf.array(), 0, length);
		byteBuf.tail(length);
		return byteBuf;
	}

	public static ByteBuf readNullable(BinaryInput input) {
		int length = input.readVarInt();
		if (length == 0) return null;
		length--;
		ByteBuf byteBuf = ByteBufPool.allocate(length);
		input.read(byteBuf.array(), 0, length);
		byteBuf.tail(length);
		return byteBuf;
	}

	public static ByteBuf readSlice(BinaryInput input) {
		int length = input.readVarInt();
		ByteBuf result = ByteBuf.wrap(input.array(), input.pos(), input.pos() + length);
		input.pos(input.pos() + length);
		return result;
	}

	public static ByteBuf readSliceNullable(BinaryInput input) {
		int length = input.readVarInt();
		if (length == 0) return null;
		length--;
		ByteBuf result = ByteBuf.wrap(input.array(), input.pos(), input.pos() + length);
		input.pos(input.pos() + length);
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenByteBuf that = (SerializerGenByteBuf) o;

		if (wrap != that.wrap) return false;
		return nullable == that.nullable;

	}

	@Override
	public int hashCode() {
		int result = (wrap ? 1 : 0);
		result = 31 * result + (nullable ? 1 : 0);
		return result;
	}
}
