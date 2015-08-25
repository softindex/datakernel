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
import io.datakernel.serializer2.SerializerStaticCaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.codegen.FunctionDefs.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;

@SuppressWarnings("PointlessArithmeticExpression")
public class SerializerGenNullable implements SerializerGen {
	private static final Logger logger = LoggerFactory.getLogger(SerializerGenNullable.class);

	private final SerializerGen serializer;

	public SerializerGenNullable(SerializerGen serializer) {
		this.serializer = checkNotNull(serializer);
	}

	@Override
	public void getVersions(VersionsCollector versions) {
		versions.addRecursive(serializer);
	}

	@Override
	public boolean isInline() {
		return serializer.isInline();
	}

	@Override
	public Class<?> getRawType() {
		return serializer.getRawType();
	}

	@Override
	public FunctionDef serialize(FunctionDef field, SerializerGen serializerGen, int version, SerializerStaticCaller serializerCaller) {
		return choice(ifNotNull(field),
				sequence(call(arg(0), "writeByte", value((byte) 1)),
						serializerCaller.serialize(serializer, field, version),
						voidFunc()),
				sequence(call(arg(0), "writeByte", value((byte) 0)), voidFunc())
		);
	}

	@Override
	public FunctionDef deserialize(Class<?> targetType, int version, SerializerStaticCaller serializerCaller) {
		FunctionDef isNotNull = let(call(arg(0), "readByte"));
		return sequence(isNotNull, choice(cmpEq(isNotNull, value((byte) 1)),
						serializerCaller.deserialize(serializer, version, targetType),
						nullRef(targetType))
		);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenNullable that = (SerializerGenNullable) o;

		if (!serializer.equals(that.serializer)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return serializer.hashCode();
	}
}
