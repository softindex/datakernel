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

package io.datakernel.serializer2.asm;

import io.datakernel.codegen.FunctionDef;
import io.datakernel.codegen.FunctionDefs;
import io.datakernel.serializer2.SerializerStaticCaller;

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
	public FunctionDef serialize(FunctionDef field, SerializerGen serializerGen, int version, SerializerStaticCaller serializerCaller) {
		return call(arg(0), "write", call(field, "getAddress"));
	}

	@Override
	public FunctionDef deserialize(final Class<?> targetType, int version, SerializerStaticCaller serializerCaller) {
		FunctionDef local = let(FunctionDefs.newArray(byte[].class, value(4)));
		return sequence(call(arg(0), "read", local), callStatic(targetType, "getByAddress", local));
	}

}
