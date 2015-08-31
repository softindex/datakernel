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

import io.datakernel.codegen.FunctionDef;
import io.datakernel.serializer.SerializerFactory;

import java.nio.ByteBuffer;

import static io.datakernel.codegen.FunctionDefs.*;

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
	public void prepareSerializeStaticMethods(int version, SerializerFactory.StaticMethods staticMethods) {

	}

	@Override
	public FunctionDef serialize(FunctionDef value, int version, SerializerFactory.StaticMethods staticMethods) {
		FunctionDef array = call(cast(value, ByteBuffer.class), "array");
		FunctionDef position = call(cast(value, ByteBuffer.class), "position");
		FunctionDef remaining = let(call(cast(value, ByteBuffer.class), "remaining"));
		FunctionDef writeLength = call(arg(0), "writeVarInt", remaining);

		return sequence(writeLength, call(arg(0), "write", array, position, remaining));
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerFactory.StaticMethods staticMethods) {

	}

	@Override
	public FunctionDef deserialize(Class<?> targetType, int version, SerializerFactory.StaticMethods staticMethods) {
		FunctionDef length = let(call(arg(0), "readVarInt"));

		if (!wrapped) {
			FunctionDef array = let(newArray(byte[].class, length));
			return sequence(length, call(arg(0), "read", array), callStatic(ByteBuffer.class, "wrap", array));
		} else {
			FunctionDef inputBuffer = call(arg(0), "array");
			FunctionDef position = let(call(arg(0), "position"));
			FunctionDef setPosition = call(arg(0), "position", add(position, length));

			return sequence(length, setPosition, callStatic(ByteBuffer.class, "wrap", inputBuffer, position, length));
		}
	}
}
