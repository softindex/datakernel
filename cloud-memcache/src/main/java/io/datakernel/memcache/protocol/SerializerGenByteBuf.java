package io.datakernel.memcache.protocol;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.util.BinaryOutputUtils;

import static io.datakernel.codegen.Expressions.*;

public class SerializerGenByteBuf implements SerializerGen, NullableOptimization {
	private boolean nullable;

	public SerializerGenByteBuf(boolean nullable) {
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
	public Class<? extends ByteBuf> getRawType() {
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
		value = let(cast(value, getRawType()));
		Expression array = call(value, "array");
		Expression head = call(value, "head");
		Expression readRemaining = call(value, "readRemaining");

		Expression writeLength = set(off, callStatic(BinaryOutputUtils.class, "writeVarInt", byteArray, off, (!nullable ? readRemaining : inc(readRemaining))));
		Expression write = sequence(writeLength, callStatic(BinaryOutputUtils.class, "write", byteArray, off, array, head, readRemaining));

		if (!nullable) {
			return write;
		} else {
			return ifThenElse(isNull(value),
					callStatic(BinaryOutputUtils.class, "writeVarInt", byteArray, off, value(0)),
					write);
		}
	}


	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		Expression length = let(call(arg(0), "readVarInt"));
		Expression byteBuffer = let(callStatic(ByteBufPool.class, "allocate", length));
		Expression array = let(call(arg(0), "array"));
		Expression swap = call(byteBuffer, "put", array, call(arg(0), "pos"), length);

		Expression result = sequence(length, byteBuffer, array, swap, byteBuffer);
		if (!nullable) {
			return result;
		} else {
			return ifThenElse(cmpEq(length, value(0)),
					nullRef(ByteBuf.class),
					result);
		}
	}
	@Override
	public SerializerGen asNullable() {
		return new SerializerGenByteBuf(true);
	}
}
