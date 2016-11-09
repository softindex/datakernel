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

import io.datakernel.bytebuf.SerializationUtils;
import io.datakernel.codegen.*;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.SerializerBuilder;

import java.util.Arrays;
import java.util.List;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;

@SuppressWarnings("PointlessArithmeticExpression")
public final class SerializerGenList implements SerializerGen, NullableOptimization {
	private final SerializerGen valueSerializer;
	private final boolean nullable;

	public SerializerGenList(SerializerGen valueSerializer) {
		this.valueSerializer = checkNotNull(valueSerializer);
		this.nullable = false;
	}

	public SerializerGenList(SerializerGen valueSerializer, boolean nullable) {
		this.valueSerializer = checkNotNull(valueSerializer);
		this.nullable = nullable;
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
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression serialize(final Expression byteArray, final Variable off, final Expression value, final int version, final SerializerBuilder.StaticMethods staticMethods, final CompatibilityLevel compatibilityLevel) {
		Expression length = length(value);
		Expression writeLength = set(off, callStatic(SerializationUtils.class, "writeVarInt", byteArray, off, (!nullable ? length : inc(length))));
		Expression forEach = forEach(value, valueSerializer.getRawType(), new ForVar() {
			@Override
			public Expression forVar(Expression it) {
				return set(off, valueSerializer.serialize(byteArray, off, it, version, staticMethods, compatibilityLevel));
			}
		});
		if (!nullable) {
			return sequence(writeLength, forEach, off);
		} else {
			return ifThenElse(isNull(value),
					sequence(set(off, callStatic(SerializationUtils.class, "writeVarInt", byteArray, off, value(0))), off),
					sequence(writeLength, forEach, off));
		}
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression deserialize(Class<?> targetType, final int version, final SerializerBuilder.StaticMethods staticMethods, final CompatibilityLevel compatibilityLevel) {
		Expression len = let(call(arg(0), "readVarInt"));
		final Expression array = let(Expressions.newArray(Object[].class, (!nullable ? len : dec(len))));
		Expression forEach = expressionFor((!nullable ? len : dec(len)), new ForVar() {
			@Override
			public Expression forVar(Expression it) {
				return setArrayItem(array, it, valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods, compatibilityLevel));
			}
		});
		Expression asList = set((StoreDef) array, callStatic(Arrays.class, "asList", array));

		if (!nullable) {
			return sequence(array, forEach, asList, array);
		} else {
			return ifThenElse(cmpEq(len, value(0)),
					nullRef(List.class),
					sequence(array, forEach, asList, array));
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenList that = (SerializerGenList) o;

		if (nullable != that.nullable) return false;
		return !(valueSerializer != null ? !valueSerializer.equals(that.valueSerializer) : that.valueSerializer != null);

	}

	@Override
	public int hashCode() {
		int result = valueSerializer != null ? valueSerializer.hashCode() : 0;
		result = 31 * result + (nullable ? 1 : 0);
		return result;
	}

	@Override
	public SerializerGen setNullable() {
		return new SerializerGenList(valueSerializer, true);
	}
}
