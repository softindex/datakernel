package io.datakernel.memcache.protocol;

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.memcache.client.MemcacheClient;
import io.datakernel.memcache.client.MemcacheClient.Slice;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.util.BinaryInput;
import io.datakernel.serializer.util.BinaryOutputUtils;

import static io.datakernel.codegen.Expressions.*;

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
	public void getVersions(VersionsCollector versions) {
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
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
	}

	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		return callStatic(SerializerGenSlice.class,
				"write" + (nullable ? "Nullable" : ""),
				byteArray, off, cast(value, Slice.class));
	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		return callStatic(SerializerGenSlice.class,
				"read" + (nullable ? "Nullable" : ""),
				arg(0));
	}

	public static int write(byte[] output, int offset, Slice buf) {
		offset = BinaryOutputUtils.writeVarInt(output, offset, buf.length());
		offset = BinaryOutputUtils.write(output, offset, buf.array(), buf.offset(), buf.length());
		return offset;
	}

	public static int writeNullable(byte[] output, int offset, Slice buf) {
		if (buf == null) {
			output[offset] = 0;
			return offset + 1;
		} else {
			offset = BinaryOutputUtils.writeVarInt(output, offset, buf.length() + 1);
			offset = BinaryOutputUtils.write(output, offset, buf.array(), buf.offset(), buf.length());
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
