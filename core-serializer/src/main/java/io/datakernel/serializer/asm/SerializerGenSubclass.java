/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.common.Utils.of;
import static io.datakernel.serializer.asm.SerializerExpressions.readByte;
import static io.datakernel.serializer.asm.SerializerExpressions.writeByte;
import static java.util.Collections.emptySet;
import static org.objectweb.asm.Type.getType;

public class SerializerGenSubclass implements SerializerGen, NullableOptimization {
	@Override
	public SerializerGen asNullable() {
		return new SerializerGenSubclass(dataType, subclassSerializers, true, startIndex);
	}

	private final Class<?> dataType;
	private final LinkedHashMap<Class<?>, SerializerGen> subclassSerializers;
	private final boolean nullable;
	private final int startIndex;

	public SerializerGenSubclass(@NotNull Class<?> dataType, LinkedHashMap<Class<?>, SerializerGen> subclassSerializers, int startIndex) {
		this.startIndex = startIndex;
		this.dataType = dataType;
		this.subclassSerializers = new LinkedHashMap<>(subclassSerializers);
		this.nullable = false;
	}

	public SerializerGenSubclass(@NotNull Class<?> dataType, LinkedHashMap<Class<?>, SerializerGen> subclassSerializers, boolean nullable, int startIndex) {
		this.startIndex = startIndex;
		this.dataType = dataType;
		this.subclassSerializers = new LinkedHashMap<>(subclassSerializers);
		this.nullable = nullable;
	}

	@Override
	public void accept(Visitor visitor) {
		for (Class<?> subclass : subclassSerializers.keySet()) {
			visitor.visit(subclass.getName(), subclassSerializers.get(subclass));
		}
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
	}

	@Override
	public boolean isInline() {
		return false;
	}

	@Override
	public Class<?> getRawType() {
		return dataType;
	}

	@Override
	public Expression serialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		byte subClassIndex = (byte) (nullable && startIndex == 0 ? 1 : startIndex);

		List<Expression> listKey = new ArrayList<>();
		List<Expression> listValue = new ArrayList<>();
		for (Class<?> subclass : subclassSerializers.keySet()) {
			SerializerGen subclassSerializer = subclassSerializers.get(subclass);
			listKey.add(cast(value(getType(subclass)), Object.class));
			listValue.add(sequence(
					writeByte(byteArray, off, value(subClassIndex)),
					subclassSerializer.serialize(classLoader, byteArray, off, cast(value, subclassSerializer.getRawType()), version, compatibilityLevel)
			));

			subClassIndex++;
			if (nullable && subClassIndex == 0) {
				subClassIndex++;
			}
		}
		if (nullable) {
			return ifThenElse(isNotNull(value),
					switchByKey(cast(call(cast(value, Object.class), "getClass"), Object.class), listKey, listValue),
					writeByte(byteArray, off, value((byte) 0)));
		} else {
			return switchByKey(cast(call(cast(value, Object.class), "getClass"), Object.class), listKey, listValue);
		}
	}

	@Override
	public Expression deserialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		return let(sub(readByte(byteArray, off), value(startIndex)),
				idx -> cast(
						switchByIndex(idx,
								of(() -> {
									List<Expression> versions = new ArrayList<>();
									for (SerializerGen subclassSerializer : subclassSerializers.values()) {
										versions.add(cast(subclassSerializer.deserialize(classLoader, byteArray, off, subclassSerializer.getRawType(), version, compatibilityLevel), dataType));
									}
									if (nullable) versions.add(-startIndex, nullRef(getRawType()));
									return versions;
								})),
						dataType));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenSubclass that = (SerializerGenSubclass) o;

		if (!dataType.equals(that.dataType)) return false;
		return subclassSerializers.equals(that.subclassSerializers);
	}

	@Override
	public int hashCode() {
		int result = dataType.hashCode();
		result = 31 * result + subclassSerializers.hashCode();
		return result;
	}
}
