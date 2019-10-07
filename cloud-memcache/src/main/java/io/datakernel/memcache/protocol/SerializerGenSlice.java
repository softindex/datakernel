package io.datakernel.memcache.protocol;

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.memcache.protocol.MemcacheRpcMessage.Slice;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.util.BinaryInput;
import io.datakernel.serializer.util.BinaryOutputUtils;

import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static java.util.Collections.emptySet;

@SuppressWarnings("unused")
public class SerializerGenSlice implements SerializerGen, NullableOptimization {
	private final boolean nullable;

	public SerializerGenSlice() {
		this.nullable = false;
	}

	SerializerGenSlice(boolean nullable) {
		this.nullable = nullable;
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
		return Slice.class;
	}

	@Override
	public Expression serialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return callStatic(SerializerGenSlice.class,
				"write" + (nullable ? "Nullable" : ""),
				byteArray, off, cast(value, Slice.class));
	}

	@Override
	public Expression deserialize(DefiningClassLoader classLoader, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		return callStatic(SerializerGenSlice.class,
				"read" + (nullable ? "Nullable" : ""),
				arg(0));
	}

	public static int write(byte[] output, int offset, Slice slice) {
		offset = BinaryOutputUtils.writeVarInt(output, offset, slice.length());
		offset = BinaryOutputUtils.write(output, offset, slice.array(), slice.offset(), slice.length());
		return offset;
	}

	public static int writeNullable(byte[] output, int offset, Slice slice) {
		if (slice == null) {
			output[offset] = 0;
			return offset + 1;
		} else {
			offset = BinaryOutputUtils.writeVarInt(output, offset, slice.length() + 1);
			offset = BinaryOutputUtils.write(output, offset, slice.array(), slice.offset(), slice.length());
			return offset;
		}
	}

	public static Slice read(BinaryInput input) {
		int length = input.readVarInt();
		Slice result = new Slice(input.array(), input.pos(), length);
		input.pos(input.pos() + length);
		return result;
	}

	public static Slice readNullable(BinaryInput input) {
		int length = input.readVarInt();
		if (length == 0) return null;
		length--;
		Slice result = new Slice(input.array(), input.pos(), length);
		input.pos(input.pos() + length);
		return result;
	}

	@Override
	public SerializerGen asNullable() {
		return new SerializerGenSlice(true);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenSlice that = (SerializerGenSlice) o;

		return nullable == that.nullable;

	}

	@Override
	public int hashCode() {
		return nullable ? 1 : 0;
	}
}
