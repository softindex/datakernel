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
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.SerializerBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;
import static org.objectweb.asm.Type.getType;

@SuppressWarnings("PointlessArithmeticExpression")
public class SerializerGenSubclass implements SerializerGen, NullableOptimization {
	@Override
	public SerializerGen setNullable() {
		return new SerializerGenSubclass(dataType, subclassSerializers, true, startIndex);
	}

	private final Class<?> dataType;
	private final LinkedHashMap<Class<?>, SerializerGen> subclassSerializers;
	private final boolean nullable;
	private final int startIndex;

	public SerializerGenSubclass(Class<?> dataType, LinkedHashMap<Class<?>, SerializerGen> subclassSerializers, int startIndex) {
		this.startIndex = startIndex;
		this.dataType = checkNotNull(dataType);
		this.subclassSerializers = new LinkedHashMap<>(subclassSerializers);
		this.nullable = false;
	}

	public SerializerGenSubclass(Class<?> dataType, LinkedHashMap<Class<?>, SerializerGen> subclassSerializers, boolean nullable, int startIndex) {
		this.startIndex = startIndex;
		this.dataType = checkNotNull(dataType);
		this.subclassSerializers = new LinkedHashMap<>(subclassSerializers);
		this.nullable = nullable;
	}

	@Override
	public void getVersions(VersionsCollector versions) {
		for (SerializerGen serializer : subclassSerializers.values()) {
			versions.addRecursive(serializer);
		}
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
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		if (staticMethods.startSerializeStaticMethod(this, version)) {
			return;
		}

		byte subClassIndex = (byte) (nullable && startIndex == 0 ? 1 : startIndex);

		List<Expression> listKey = new ArrayList<>();
		List<Expression> listValue = new ArrayList<>();
		for (Class<?> subclass : subclassSerializers.keySet()) {
			SerializerGen subclassSerializer = subclassSerializers.get(subclass);
			subclassSerializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);
			listKey.add(cast(value(getType(subclass)), Object.class));
			listValue.add(sequence(
					set(arg(1), callStatic(SerializationUtils.class, "writeByte", arg(0), arg(1), value(subClassIndex))),
					subclassSerializer.serialize(arg(0), arg(1), cast(arg(2), subclassSerializer.getRawType()), version, staticMethods, compatibilityLevel)
			));

			subClassIndex++;
			if (nullable && subClassIndex == 0) {
				subClassIndex++;
			}
		}
		if (nullable) {
			staticMethods.registerStaticSerializeMethod(this, version,
					ifThenElse(isNotNull(arg(2)),
							switchForKey(cast(call(cast(arg(2), Object.class), "getClass"), Object.class), listKey, listValue),
							callStatic(SerializationUtils.class, "writeByte", arg(0), arg(1), value((byte) 0)))
			);
		} else {
			staticMethods.registerStaticSerializeMethod(this, version,
					switchForKey(cast(call(cast(arg(2), Object.class), "getClass"), Object.class), listKey, listValue)
			);
		}
	}

	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		return staticMethods.callStaticSerializeMethod(this, version, byteArray, off, value);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		if (staticMethods.startDeserializeStaticMethod(this, version)) {
			return;
		}
		List<Expression> list = new ArrayList<>();
		for (Class<?> subclass : subclassSerializers.keySet()) {
			SerializerGen subclassSerializer = subclassSerializers.get(subclass);
			subclassSerializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
			list.add(cast(subclassSerializer.deserialize(subclassSerializer.getRawType(), version, staticMethods, compatibilityLevel), this.getRawType()));
		}
		if (nullable) list.add(-startIndex, nullRef(getRawType()));

		Variable subClassIndex = let(sub(call(arg(0), "readByte"), value(startIndex)));

		staticMethods.registerStaticDeserializeMethod(this, version, cast(switchForPosition(subClassIndex, list), this.getRawType()));
	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		return staticMethods.callStaticDeserializeMethod(this, version, arg(0));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenSubclass that = (SerializerGenSubclass) o;

		if (!dataType.equals(that.dataType)) return false;
		if (!subclassSerializers.equals(that.subclassSerializers)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = dataType.hashCode();
		result = 31 * result + subclassSerializers.hashCode();
		return result;
	}
}
