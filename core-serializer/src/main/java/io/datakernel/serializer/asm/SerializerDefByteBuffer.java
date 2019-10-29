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
import io.datakernel.serializer.HasNullable;

import java.nio.ByteBuffer;
import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.serializer.asm.SerializerExpressions.*;
import static java.util.Collections.emptySet;

public class SerializerDefByteBuffer implements SerializerDef, HasNullable {
	private final boolean wrapped;
	private final boolean nullable;

	public SerializerDefByteBuffer() {
		this(false);
	}

	public SerializerDefByteBuffer(boolean wrapped) {
		this.wrapped = wrapped;
		this.nullable = false;
	}

	private SerializerDefByteBuffer(boolean wrapped, boolean nullable) {
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
	public Class<?> getRawType() {
		return ByteBuffer.class;
	}

	@Override
	public Expression encoder(DefiningClassLoader classLoader, StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return let(
				cast(value, ByteBuffer.class),
				buffer ->
						let(call(buffer, "remaining"), remaining -> {

							if (!nullable) {
								return sequence(
										writeVarInt(buf, pos, remaining),
										writeBytes(buf, pos, call(buffer, "array"), call(buffer, "position"), remaining));
							} else {
								return ifThenElse(isNull(buffer),
										writeByte(buf, pos, value((byte) 0)),
										sequence(
												writeVarInt(buf, pos, inc(remaining)),
												writeBytes(buf, pos, call(buffer, "array"), call(buffer, "position"), remaining)));
							}
						}));
	}

	@Override
	public Expression decoder(DefiningClassLoader classLoader, StaticDecoders staticDecoders, Expression in, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		return !wrapped ?
				let(readVarInt(in),
						length -> {
							if (!nullable) {
								return let(
										newArray(byte[].class, length),
										array ->
												sequence(length,
														readBytes(in, array),
														callStatic(ByteBuffer.class, "wrap", array)));
							} else {
								return let(
										newArray(byte[].class, dec(length)),
										array ->
												ifThenElse(cmpEq(length, value(0)),
														nullRef(ByteBuffer.class),
														sequence(length,
																readBytes(in, array),
																callStatic(ByteBuffer.class, "wrap", array))));
							}
						}) :
				let(readVarInt(in),
						length -> {
							if (!nullable) {
								return let(callStatic(ByteBuffer.class, "wrap", array(in), pos(in), length),
										buf -> sequence(
												move(in, length),
												buf));
							} else {
								return ifThenElse(cmpEq(length, value(0)),
										nullRef(ByteBuffer.class),
										let(callStatic(ByteBuffer.class, "wrap", array(in), pos(in), dec(length)),
												result -> sequence(
														move(in, length),
														result)));
							}
						});
	}

	@Override
	public SerializerDef withNullable() {
		return new SerializerDefByteBuffer(wrapped, true);
	}
}
