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

import io.datakernel.codegen.*;
import io.datakernel.serializer.SerializerBuilder;

import java.util.Arrays;
import java.util.List;

import static io.datakernel.codegen.Expressions.*;
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
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression serialize(Expression value, final int version, final SerializerBuilder.StaticMethods staticMethods) {
		Expression length = let(length(value));
		Expression len = call(arg(0), "writeVarInt", length);
		ExpressionListForEach forEach = listForEach(value, new ForVar() {
			@Override
			public Expression forVar(Expression item) {
				return valueSerializer.serialize(cast(item, valueSerializer.getRawType()), version, staticMethods);
			}
		});

		return sequence(len, forEach);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression deserialize(Class<?> targetType, final int version, final SerializerBuilder.StaticMethods staticMethods) {
		Expression local = let(Expressions.newArray(Object[].class, call(arg(0), "readVarInt")));
		ExpressionArrayForEachWithChanges forEach = arrayForEachWithChanges(local, new ForEachWithChanges() {
			@Override
			public Expression forEachWithChanges() {
				return valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods);
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
