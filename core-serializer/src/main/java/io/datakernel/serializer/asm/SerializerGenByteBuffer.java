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

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.SerializerBuilder.StaticMethods;
import io.datakernel.serializer.util.BinaryOutputUtils;

import java.nio.ByteBuffer;

import static io.datakernel.codegen.Expressions.*;

public class SerializerGenByteBuffer implements SerializerGen, NullableOptimization {
	private final boolean wrapped;
	private final boolean nullable;

	public SerializerGenByteBuffer() {
		this(false);
	}

	public SerializerGenByteBuffer(boolean wrapped) {
		this.wrapped = wrapped;
		this.nullable = false;
	}

	private SerializerGenByteBuffer(boolean wrapped, boolean nullable) {
		this.wrapped = wrapped;
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
		return ByteBuffer.class;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		return let(
				cast(value, ByteBuffer.class),
				buffer ->
						let(call(buffer, "remaining"), remaining -> {
							Expression write = sequence(
									set(off,
											callStatic(BinaryOutputUtils.class, "writeVarInt",
													byteArray,
													off,
													!nullable ? remaining : inc(remaining))),
									callStatic(BinaryOutputUtils.class, "write",
											byteArray,
											off,
											call(buffer, "array"),
											call(buffer, "position"),
											remaining));

							if (!nullable) {
								return write;
							} else {
								return ifThenElse(isNull(buffer),
										callStatic(BinaryOutputUtils.class, "writeByte",
												byteArray,
												off,
												value((byte) 0)),
										write);
							}
						}));
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		return !wrapped ?
				let(call(arg(0), "readVarInt"), length -> {
					if (!nullable) {
						return let(
								newArray(byte[].class, length),
								array ->
										sequence(length, call(arg(0), "read", array), callStatic(ByteBuffer.class, "wrap", array)));
					} else {
						return let(
								newArray(byte[].class, dec(length)),
								array ->
										ifThenElse(cmpEq(length, value(0)),
												nullRef(ByteBuffer.class),
												sequence(length, call(arg(0), "read", array), callStatic(ByteBuffer.class, "wrap", array))));
					}
				}) :
				let(call(arg(0), "readVarInt"), length ->
						let(call(arg(0), "pos"), position -> {
							Expression setPosition = call(arg(0), "pos", add(position, (!nullable ? length : dec(length))));
							Expression inputBuffer = property(arg(0), "array");

							if (!nullable) {
								return sequence(setPosition, callStatic(ByteBuffer.class, "wrap", inputBuffer, position, length));
							} else {
								return ifThenElse(cmpEq(length, value(0)),
										nullRef(ByteBuffer.class),
										sequence(setPosition, callStatic(ByteBuffer.class, "wrap", inputBuffer, position, dec(length))));
							}
						}));
	}

	@Override
	public SerializerGen asNullable() {
		return new SerializerGenByteBuffer(wrapped, true);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenByteBuffer that = (SerializerGenByteBuffer) o;

		if (wrapped != that.wrapped) return false;
		return nullable == that.nullable;

	}

	@Override
	public int hashCode() {
		int result = wrapped ? 1 : 0;
		result = 31 * result + (nullable ? 1 : 0);
		return result;
	}
}
