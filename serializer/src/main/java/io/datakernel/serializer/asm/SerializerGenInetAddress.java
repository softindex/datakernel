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
import io.datakernel.codegen.FunctionDefs;
import io.datakernel.serializer.SerializerFactory;

import java.net.InetAddress;

import static io.datakernel.codegen.FunctionDefs.*;

@SuppressWarnings("PointlessArithmeticExpression")
public class SerializerGenInetAddress implements SerializerGen {
	private static final SerializerGenInetAddress INSTANCE = new SerializerGenInetAddress();

	public static SerializerGenInetAddress instance() {
		return INSTANCE;
	}

	private SerializerGenInetAddress() {
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
		return InetAddress.class;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerFactory.StaticMethods staticMethods) {

	}

	@Override
	public FunctionDef serialize(FunctionDef value, int version, SerializerFactory.StaticMethods staticMethods) {
		return call(arg(0), "write", call(value, "getAddress"));
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerFactory.StaticMethods staticMethods) {

	}

	@Override
	public FunctionDef deserialize(Class<?> targetType, int version, SerializerFactory.StaticMethods staticMethods) {
		FunctionDef local = let(FunctionDefs.newArray(byte[].class, value(4)));
		return sequence(call(arg(0), "read", local), callStatic(targetType, "getByAddress", local));
	}

}
