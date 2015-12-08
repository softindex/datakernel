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
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.SerializerUtils;

import java.net.Inet6Address;

import static io.datakernel.codegen.Expressions.*;

public class SerializerGenInet6Address implements SerializerGen {
	private static final SerializerGenInet6Address INSTANCE = new SerializerGenInet6Address();

	@Override
	public void getVersions(VersionsCollector versions) {

	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return Inet6Address.class;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		return callStatic(SerializerUtils.class, "write", call(cast(value, getRawType()), "getAddress"), byteArray, off);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		Expression local = let(Expressions.newArray(byte[].class, value(16)));
		return sequence(call(arg(0), "read", local), callStatic(getRawType(), "getByAddress", local));
	}

	public static SerializerGen instance() {
		return INSTANCE;
	}
}
