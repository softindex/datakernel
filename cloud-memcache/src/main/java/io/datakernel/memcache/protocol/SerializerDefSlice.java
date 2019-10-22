package io.datakernel.memcache.protocol;

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.memcache.protocol.MemcacheRpcMessage.Slice;
import io.datakernel.serializer.BinaryInput;
import io.datakernel.serializer.BinaryOutputUtils;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.HasNullable;
import io.datakernel.serializer.asm.SerializerDef;

import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static java.util.Collections.emptySet;

@SuppressWarnings("unused")
public class SerializerDefSlice implements SerializerDef, HasNullable {
	private final boolean nullable;

	public SerializerDefSlice() {
		this.nullable = false;
	}

	SerializerDefSlice(boolean nullable) {
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
	public Class<?> getRawType() {
		return Slice.class;
	}

	@Override
	public Expression encoder(DefiningClassLoader classLoader, StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return set(pos,
				callStatic(SerializerDefSlice.class,
						"write" + (nullable ? "Nullable" : ""),
						buf, pos, cast(value, Slice.class)));
	}

	@Override
	public Expression decoder(DefiningClassLoader classLoader, StaticDecoders staticDecoders, Expression in, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		return callStatic(SerializerDefSlice.class,
				"read" + (nullable ? "Nullable" : ""),
				in);
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

	public static Slice read(BinaryInput in) {
		int length = in.readVarInt();
		Slice result = new Slice(in.array(), in.pos(), length);
		in.pos(in.pos() + length);
		return result;
	}

	public static Slice readNullable(BinaryInput in) {
		int length = in.readVarInt();
		if (length == 0) return null;
		length--;
		Slice result = new Slice(in.array(), in.pos(), length);
		in.pos(in.pos() + length);
		return result;
	}

	@Override
	public SerializerDef withNullable() {
		return new SerializerDefSlice(true);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerDefSlice that = (SerializerDefSlice) o;

		return nullable == that.nullable;

	}

	@Override
	public int hashCode() {
		return nullable ? 1 : 0;
	}
}
