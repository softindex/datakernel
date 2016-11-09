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
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Expressions;
import io.datakernel.codegen.ForVar;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.SerializerBuilder;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;

@SuppressWarnings("PointlessArithmeticExpression")
public final class SerializerGenArray implements SerializerGen, NullableOptimization {
	private final SerializerGen valueSerializer;
	private final int fixedSize;
	private final Class<?> type;
	private final boolean nullable;

	public SerializerGenArray(SerializerGen serializer, int fixedSize, Class<?> type, boolean nullable) {
		this.valueSerializer = checkNotNull(serializer);
		this.fixedSize = fixedSize;
		this.type = type;
		this.nullable = nullable;
	}

	public SerializerGenArray(SerializerGen serializer, int fixedSize, Class<?> type) {
		this.valueSerializer = checkNotNull(serializer);
		this.fixedSize = fixedSize;
		this.type = type;
		this.nullable = false;
	}

	public SerializerGenArray(SerializerGen serializer, Class<?> type) {
		this(serializer, -1, type);
	}

	public SerializerGenArray fixedSize(int fixedSize, Class<?> nameOfClass) {
		return new SerializerGenArray(valueSerializer, fixedSize, nameOfClass, nullable);
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
		return Object.class;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression serialize(final Expression byteArray, final Variable off, Expression value, final int version,
	                            final SerializerBuilder.StaticMethods staticMethods,
	                            final CompatibilityLevel compatibilityLevel) {
		final Expression castedValue = cast(value, type);
		Expression length = (fixedSize != -1
				? value(fixedSize)
				: length(castedValue));

		Expression writeBytes = callStatic(SerializationUtils.class, "write", byteArray, off, castedValue);
		Expression writeZero = set(off, callStatic(SerializationUtils.class, "writeVarInt", byteArray, off, value(0)));
		Expression writeLength = set(off, callStatic(SerializationUtils.class, "writeVarInt", byteArray, off, (!nullable ? length : inc(length))));
		Expression expressionFor = expressionFor(length, new ForVar() {
			@Override
			public Expression forVar(Expression it) {
				return set(off, valueSerializer.serialize(byteArray, off, getArrayItem(castedValue, it), version, staticMethods, compatibilityLevel));
			}
		});

		if (!nullable) {
			if (type.getComponentType() == Byte.TYPE) {
				return sequence(writeLength, writeBytes);
			} else {
				return sequence(writeLength, expressionFor, off);
			}
		} else {
			if (type.getComponentType() == Byte.TYPE) {
				return ifThenElse(isNull(value),
						sequence(writeZero, off),
						sequence(writeLength, writeBytes)
				);
			} else {
				return ifThenElse(isNull(value),
						sequence(writeZero, off),
						sequence(writeLength, expressionFor, off));
			}
		}
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression deserialize(Class<?> targetType, final int version, final SerializerBuilder.StaticMethods staticMethods, final CompatibilityLevel compatibilityLevel) {
		Expression len = let(call(arg(0), "readVarInt"));
		final Expression array = let(Expressions.newArray(type, (!nullable ? len : dec(len))));
		Expression expressionFor = expressionFor((!nullable ? len : dec(len)), new ForVar() {
			@Override
			public Expression forVar(Expression it) {
				return setArrayItem(array, it, cast(valueSerializer.deserialize(type.getComponentType(), version, staticMethods, compatibilityLevel), type.getComponentType()));
			}
		});

		if (!nullable) {
			if (type.getComponentType() == Byte.TYPE) {
				return sequence(call(arg(0), "read", array), array);
			} else {
				return sequence(array, expressionFor, array);
			}
		} else {
			if (type.getComponentType() == Byte.TYPE) {
				return ifThenElse(cmpEq(len, value(0)),
						nullRef(type),
						sequence(call(arg(0), "read", array), array));
			} else {
				return ifThenElse(cmpEq(len, value(0)),
						nullRef(type),
						sequence(array, expressionFor, array));
			}
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenArray that = (SerializerGenArray) o;

		if (fixedSize != that.fixedSize) return false;
		if (nullable != that.nullable) return false;
		if (valueSerializer != null ? !valueSerializer.equals(that.valueSerializer) : that.valueSerializer != null)
			return false;
		return !(type != null ? !type.equals(that.type) : that.type != null);

	}

	@Override
	public int hashCode() {
		int result = valueSerializer != null ? valueSerializer.hashCode() : 0;
		result = 31 * result + fixedSize;
		result = 31 * result + (type != null ? type.hashCode() : 0);
		result = 31 * result + (nullable ? 1 : 0);
		return result;
	}

	@Override
	public SerializerGen setNullable() {
		return new SerializerGenArray(valueSerializer, fixedSize, type, true);
	}
}
