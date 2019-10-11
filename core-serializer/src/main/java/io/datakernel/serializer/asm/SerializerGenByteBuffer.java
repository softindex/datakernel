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

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;

import java.nio.ByteBuffer;
import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.serializer.asm.SerializerExpressions.*;
import static java.util.Collections.emptySet;

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
		return ByteBuffer.class;
	}

	@Override
	public Expression serialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return let(
				cast(value, ByteBuffer.class),
				buffer ->
						let(call(buffer, "remaining"), remaining -> {

							if (!nullable) {
								return sequence(
										writeVarInt(byteArray, off, remaining),
										writeBytes(byteArray, off, call(buffer, "array"), call(buffer, "position"), remaining));
							} else {
								return ifThenElse(isNull(buffer),
										writeByte(byteArray, off, value((byte) 0)),
										sequence(
												writeVarInt(byteArray, off, inc(remaining)),
												writeBytes(byteArray, off, call(buffer, "array"), call(buffer, "position"), remaining)));
							}
						}));
	}

	@Override
	public Expression deserialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		return !wrapped ?
				let(readVarInt(byteArray, off),
						length -> {
							if (!nullable) {
								return let(
										newArray(byte[].class, length),
										array ->
												sequence(length,
														readBytes(byteArray, off, array),
														callStatic(ByteBuffer.class, "wrap", array)));
							} else {
								return let(
										newArray(byte[].class, dec(length)),
										array ->
												ifThenElse(cmpEq(length, value(0)),
														nullRef(ByteBuffer.class),
														sequence(length,
																readBytes(byteArray, off, array),
																callStatic(ByteBuffer.class, "wrap", array))));
							}
						}) :
				let(readVarInt(byteArray, off),
						length -> {
							if (!nullable) {
								return let(callStatic(ByteBuffer.class, "wrap", byteArray, off, length),
										result -> sequence(
												set(off, add(off, length)),
												result));
							} else {
								return ifThenElse(cmpEq(length, value(0)),
										nullRef(ByteBuffer.class),
										let(callStatic(ByteBuffer.class, "wrap", byteArray, off, dec(length)),
												result -> sequence(
														set(off, add(off, dec(length))),
														result)));
							}
						});
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
