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

import io.datakernel.codegen.ForVar;
import io.datakernel.codegen.FunctionDef;
import io.datakernel.serializer2.SerializerStaticCaller;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.datakernel.codegen.FunctionDefs.*;
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
	public FunctionDef serialize(FunctionDef field, final SerializerGen serializerGen, final int version, final SerializerStaticCaller serializerCaller) {
		FunctionDef length = call(arg(0), "writeVarInt", length(field));

		return sequence(length, mapForEach(field,
						new ForVar() {
							@Override
							public FunctionDef forVar(FunctionDef item) { return serializerCaller.serialize(keySerializer, cast(item, keySerializer.getRawType()), version);}
						},
						new ForVar() {
							@Override
							public FunctionDef forVar(FunctionDef item) {return serializerCaller.serialize(valueSerializer, cast(item, valueSerializer.getRawType()), version);}
						})
		);
	}

	@Override
	public FunctionDef deserialize(Class<?> targetType, final int version, final SerializerStaticCaller serializerCaller) {
		boolean isEnum = keySerializer.getRawType().isEnum();

		if (!isEnum) {
			return deserializeSimple(targetType, version, serializerCaller);
		} else {
			return deserializeEnum(targetType, version, serializerCaller);
		}

	}

	public FunctionDef deserializeSimple(Class<?> targetType, final int version, final SerializerStaticCaller serializerCaller) {
		FunctionDef length = let(call(arg(0), "readVarInt"));
		final FunctionDef local = let(constructor(LinkedHashMap.class, length));
		FunctionDef forEach = functionFor(length, new ForVar() {
			@Override
			public FunctionDef forVar(FunctionDef eachNumber) {
				return sequence(call(local, "put",
						cast(keySerializer.deserialize(keySerializer.getRawType(), version, serializerCaller), Object.class),
						cast(valueSerializer.deserialize(valueSerializer.getRawType(), version, serializerCaller), Object.class)
				), voidFunc());
			}
		});
		return sequence(length, local, forEach, local);
	}

	public FunctionDef deserializeEnum(Class<?> targetType, final int version, final SerializerStaticCaller serializerCaller) {
		FunctionDef length = let(call(arg(0), "readVarInt"));
		final FunctionDef localMap = let(constructor(EnumMap.class, cast(value(getType(keySerializer.getRawType())), Class.class)));
		FunctionDef forEach = functionFor(length, new ForVar() {
			@Override
			public FunctionDef forVar(FunctionDef eachNumber) {
				return sequence(call(localMap, "put",
						cast(keySerializer.deserialize(keySerializer.getRawType(), version, serializerCaller), Object.class),
						cast(valueSerializer.deserialize(valueSerializer.getRawType(), version, serializerCaller), Object.class)
				), voidFunc());
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
