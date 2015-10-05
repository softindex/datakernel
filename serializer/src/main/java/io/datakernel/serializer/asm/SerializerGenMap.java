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
import io.datakernel.codegen.ForVar;
import io.datakernel.serializer.SerializerBuilder;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;
import static org.objectweb.asm.Type.getType;

@SuppressWarnings("PointlessArithmeticExpression")
public final class SerializerGenMap implements SerializerGen {
	private final SerializerGen keySerializer;
	private final SerializerGen valueSerializer;

	public SerializerGenMap(SerializerGen keySerializer, SerializerGen valueSerializer) {
		this.keySerializer = checkNotNull(keySerializer);
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
		return Map.class;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		keySerializer.prepareSerializeStaticMethods(version, staticMethods);
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression serialize(Expression value, final int version, final SerializerBuilder.StaticMethods staticMethods) {
		Expression length = call(arg(0), "writeVarInt", length(value));

		return sequence(length, mapForEach(value,
				new ForVar() {
					@Override
					public Expression forVar(Expression item) {return keySerializer.serialize(cast(item, keySerializer.getRawType()), version, staticMethods);}
				},
				new ForVar() {
					@Override
					public Expression forVar(Expression item) {return valueSerializer.serialize(cast(item, valueSerializer.getRawType()), version, staticMethods);}
				})
		);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		keySerializer.prepareDeserializeStaticMethods(version, staticMethods);
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression deserialize(Class<?> targetType, final int version, SerializerBuilder.StaticMethods staticMethods) {
		boolean isEnum = keySerializer.getRawType().isEnum();

		if (!isEnum) {
			return deserializeSimple(version, staticMethods);
		} else {
			return deserializeEnum(version, staticMethods);
		}
	}

	public Expression deserializeSimple(final int version, final SerializerBuilder.StaticMethods staticMethods) {
		Expression length = let(call(arg(0), "readVarInt"));
		final Expression local = let(constructor(LinkedHashMap.class, length));
		Expression forEach = expressionFor(length, new ForVar() {
			@Override
			public Expression forVar(Expression eachNumber) {
				return sequence(call(local, "put",
						cast(keySerializer.deserialize(keySerializer.getRawType(), version, staticMethods), Object.class),
						cast(valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods), Object.class)
				), voidExp());
			}
		});
		return sequence(length, local, forEach, local);
	}

	public Expression deserializeEnum(final int version, final SerializerBuilder.StaticMethods staticMethods) {
		Expression length = let(call(arg(0), "readVarInt"));
		final Expression localMap = let(constructor(EnumMap.class, cast(value(getType(keySerializer.getRawType())), Class.class)));
		Expression forEach = expressionFor(length, new ForVar() {
			@Override
			public Expression forVar(Expression eachNumber) {
				return sequence(call(localMap, "put",
						cast(keySerializer.deserialize(keySerializer.getRawType(), version, staticMethods), Object.class),
						cast(valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods), Object.class)
				), voidExp());
			}
		});
		return sequence(length, localMap, forEach, localMap);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenMap that = (SerializerGenMap) o;

		if (!keySerializer.equals(that.keySerializer)) return false;
		if (!valueSerializer.equals(that.valueSerializer)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = keySerializer.hashCode();
		result = 31 * result + valueSerializer.hashCode();
		return result;
	}
}
