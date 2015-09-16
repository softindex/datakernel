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
import io.datakernel.codegen.ForEachWithChanges;
import io.datakernel.codegen.ForVar;
import io.datakernel.serializer.SerializerBuilder;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;

@SuppressWarnings("PointlessArithmeticExpression")
public final class SerializerGenArray implements SerializerGen {
	private final SerializerGen valueSerializer;
	private final int fixedSize;
	private Class<?> type;

	public SerializerGenArray(SerializerGen serializer, int fixedSize, Class<?> type) {
		this.valueSerializer = checkNotNull(serializer);
		this.fixedSize = fixedSize;
		this.type = type;
	}

	public SerializerGenArray(SerializerGen serializer, Class<?> type) {
		this(serializer, -1, type);
	}

	public SerializerGenArray fixedSize(int fixedSize, Class<?> nameOfClass) {
		return new SerializerGenArray(valueSerializer, fixedSize, nameOfClass);
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
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression serialize(Expression value, final int version, final SerializerBuilder.StaticMethods staticMethods) {
		value = cast(value, type);
		Expression len = call(arg(0), "writeVarInt", length(value));
		if (fixedSize != -1) len = value(fixedSize);

		if (type.getComponentType() == Byte.TYPE) {
			return sequence(len, call(arg(0), "write", value));
		} else {
			return sequence(len, arrayForEach(value, new ForVar() {
				@Override
				public Expression forVar(Expression item) {
					return valueSerializer.serialize(item, version, staticMethods);
				}
			}));
		}
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression deserialize(Class<?> targetType, final int version, final SerializerBuilder.StaticMethods staticMethods) {
		Expression len = call(arg(0), "readVarInt");
		if (fixedSize != -1) len = value(fixedSize);

		Expression array = let(Expressions.newArray(type, len));
		if (type.getComponentType() == Byte.TYPE) {
			return sequence(call(arg(0), "read", array), array);
		} else {
			return sequence(arrayForEachWithChanges(array, new ForEachWithChanges() {
				@Override
				public Expression forEachWithChanges() {
					return cast(valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods), type.getComponentType());
				}
			}), array);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenArray that = (SerializerGenArray) o;

		if (fixedSize != that.fixedSize) return false;
		if (!valueSerializer.equals(that.valueSerializer)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = valueSerializer.hashCode();
		result = 31 * result + fixedSize;
		return result;
	}
}
