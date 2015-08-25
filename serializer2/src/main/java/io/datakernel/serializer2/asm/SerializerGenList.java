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

import io.datakernel.codegen.*;
import io.datakernel.serializer2.SerializerStaticCaller;

import java.util.Arrays;
import java.util.List;

import static io.datakernel.codegen.FunctionDefs.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;

@SuppressWarnings("PointlessArithmeticExpression")
public final class SerializerGenList implements SerializerGen {
	private final SerializerGen valueSerializer;

	public SerializerGenList(SerializerGen valueSerializer) {
		this.valueSerializer = checkNotNull(valueSerializer);
	}

	@Override
	public void getVersions(VersionsCollector versions) {
		versions.addRecursive(valueSerializer);
	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return List.class;
	}

	@Override
	public FunctionDef serialize(FunctionDef field, final SerializerGen serializerGen, final int version, final SerializerStaticCaller serializerCaller) {
		FunctionDef length = let(length(field));
		FunctionDef len = call(arg(0), "writeVarInt", length);
		FunctionDefListForEach forEach = listForEach(field, new ForVar() {
			@Override
			public FunctionDef forVar(FunctionDef item) {
				return serializerCaller.serialize(valueSerializer, cast(item, valueSerializer.getRawType()), version);
			}
		});

		return sequence(len, forEach);
	}

	@Override
	public FunctionDef deserialize(final Class<?> targetType, final int version, final SerializerStaticCaller serializerCaller) {
		FunctionDef local = let(FunctionDefs.newArray(Object[].class, call(arg(0), "readVarInt")));
		FunctionDefArrayForEachWithChanges forEach = arrayForEachWithChanges(local, new ForEachWithChanges() {
			@Override
			public FunctionDef forEachWithChanges() {
				return serializerCaller.deserialize(valueSerializer, version, valueSerializer.getRawType());
			}
		});

		return sequence(forEach, set((StoreDef) local, callStatic(Arrays.class, "asList", local)), local);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenList that = (SerializerGenList) o;

		if (!valueSerializer.equals(that.valueSerializer)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return valueSerializer.hashCode();
	}
}
