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
import io.datakernel.codegen.ExpressionLet;
import io.datakernel.codegen.Variable;
import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializationOutputBuffer;
import io.datakernel.serializer.SerializerBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;

@SuppressWarnings("PointlessArithmeticExpression")
public class SerializerGenSubclass implements SerializerGen {
	public static final class Builder {
		private final Class<?> dataType;
		private boolean nullable;
		private final LinkedHashMap<Class<?>, SerializerGen> subclassSerializers = new LinkedHashMap<>();

		public Builder(Class<?> dataType) {
			this.dataType = dataType;
		}

		public Builder add(Class<?> subclass, SerializerGen serializer) {
			Preconditions.check(subclassSerializers.put(subclass, serializer) == null);
			return this;
		}

		public Builder nullable(boolean nullable) {
			this.nullable = nullable;
			return this;
		}

		public SerializerGenSubclass build() {
			return new SerializerGenSubclass(dataType, subclassSerializers, nullable);
		}
	}

	private final Class<?> dataType;
	private final LinkedHashMap<Class<?>, SerializerGen> subclassSerializers;
	private final boolean nullable;

	public SerializerGenSubclass(Class<?> dataType, LinkedHashMap<Class<?>, SerializerGen> subclassSerializers) {
		this.dataType = checkNotNull(dataType);
		this.subclassSerializers = new LinkedHashMap<>(subclassSerializers);
		this.nullable = false;
	}

	public SerializerGenSubclass(Class<?> dataType, LinkedHashMap<Class<?>, SerializerGen> subclassSerializers, boolean nullable) {
		this.dataType = checkNotNull(dataType);
		this.subclassSerializers = new LinkedHashMap<>(subclassSerializers);
		this.nullable = nullable;
	}

	public SerializerGenSubclass nullable(boolean nullable) {
		return new SerializerGenSubclass(dataType, subclassSerializers, nullable);
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

		byte subClassN = (byte) (nullable ? 1 : 0);
		List<Expression> listKey = new ArrayList<>();
		List<Expression> listValue = new ArrayList<>();
		for (Class<?> subclass : subclassSerializers.keySet()) {
			SerializerGen subclassSerializer = subclassSerializers.get(subclass);
			subclassSerializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);

			listKey.add(value(subclass.getName()));
			listValue.add(sequence(
					set(arg(1), callStatic(SerializationOutputBuffer.class, "writeByte", arg(0), arg(1), value(subClassN++))),
					subclassSerializer.serialize(arg(0), arg(1), cast(arg(2), subclassSerializer.getRawType()), version, staticMethods, compatibilityLevel)
			));
		}
		if (nullable) {
			staticMethods.registerStaticSerializeMethod(this, version,
					choice(isNotNull(arg(2)),
							switchForKey(cast(call(call(cast(arg(2), Object.class), "getClass"), "getName"), Object.class), listKey, listValue),
							callStatic(SerializationOutputBuffer.class, "writeByte", arg(0), arg(1), value((byte) 0)))
			);
		} else {
			staticMethods.registerStaticSerializeMethod(this, version,
					switchForKey(cast(call(call(cast(arg(2), Object.class), "getClass"), "getName"), Object.class), listKey, listValue)
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
		if (nullable) list.add(nullRef(getRawType()));
		for (Class<?> subclass : subclassSerializers.keySet()) {
			SerializerGen subclassSerializer = subclassSerializers.get(subclass);
			subclassSerializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
			list.add(cast(subclassSerializer.deserialize(subclassSerializer.getRawType(), version, staticMethods, compatibilityLevel), this.getRawType()));
		}

		ExpressionLet subClassN = let(call(arg(0), "readByte"));

		staticMethods.registerStaticDeserializeMethod(this, version, cast(switchForPosition(subClassN, list), this.getRawType()));
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
