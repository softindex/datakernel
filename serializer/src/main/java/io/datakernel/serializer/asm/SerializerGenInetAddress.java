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
import io.datakernel.codegen.Expressions;
import io.datakernel.serializer.SerializerBuilder;

import java.net.InetAddress;

import static io.datakernel.codegen.Expressions.*;

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
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {

	}

	@Override
	public Expression serialize(Expression value, int version, SerializerBuilder.StaticMethods staticMethods) {
		return call(arg(0), "write", call(value, "getAddress"));
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {

	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods) {
		Expression local = let(Expressions.newArray(byte[].class, value(4)));
		return sequence(call(arg(0), "read", local), callStatic(targetType, "getByAddress", local));
	}

}
