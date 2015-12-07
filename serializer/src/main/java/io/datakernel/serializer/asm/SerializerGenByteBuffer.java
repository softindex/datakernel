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
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializationOutputBuffer;
import io.datakernel.serializer.SerializerBuilder;

import java.nio.ByteBuffer;

import static io.datakernel.codegen.Expressions.*;

public class SerializerGenByteBuffer implements SerializerGen {
	private final boolean wrapped;

	public SerializerGenByteBuffer() {
		this(false);
	}

	public SerializerGenByteBuffer(boolean wrapped) {
		this.wrapped = wrapped;
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
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		value = let(cast(value, ByteBuffer.class));
		Expression array = call(value, "array");
		Expression position = call(value, "position");
		Expression remaining = let(call(value, "remaining"));
		Expression writeLength = set(off, callStatic(SerializationOutputBuffer.class, "writeVarInt", byteArray, off, remaining));

		return sequence(writeLength, callStatic(SerializationOutputBuffer.class, "write", array, position, byteArray, off, remaining));
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		Expression length = let(call(arg(0), "readVarInt"));

		if (!wrapped) {
			Expression array = let(newArray(byte[].class, length));
			return sequence(length, call(arg(0), "read", array), callStatic(ByteBuffer.class, "wrap", array));
		} else {
			Expression inputBuffer = call(arg(0), "array");
			Expression position = let(call(arg(0), "position"));
			Expression setPosition = call(arg(0), "position", add(position, length));

			return sequence(length, setPosition, callStatic(ByteBuffer.class, "wrap", inputBuffer, position, length));
		}
	}
}
