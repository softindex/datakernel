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

import io.datakernel.codegen.ForEachWithChanges;
import io.datakernel.codegen.ForVar;
import io.datakernel.codegen.FunctionDef;
import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer2.SerializerStaticCaller;

import java.util.*;

import static io.datakernel.codegen.FunctionDefs.*;

public class SerializerGenSet implements SerializerGen {
	private final SerializerGen valueSerializer;

	public SerializerGenSet(SerializerGen valueSerializer) {this.valueSerializer = valueSerializer;}

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
		return Set.class;
	}

	@Override
	public FunctionDef serialize(FunctionDef field, SerializerGen serializerGen, final int version, final SerializerStaticCaller serializerCaller) {
		return sequence(
				call(arg(0), "writeVarInt", call(field, "size")),
				setForEach(field, new ForVar() {
					@Override
					public FunctionDef forVar(FunctionDef local) {
						return sequence(serializerCaller.serialize(valueSerializer, cast(local, valueSerializer.getRawType()), version), voidFunc());
					}
				})
		);
	}

	@Override
	public FunctionDef deserialize(Class<?> targetType, final int version, final SerializerStaticCaller serializerCaller) {
		boolean isEnum = valueSerializer.getRawType().isEnum();
		Class<?> targetInstance = isEnum ? EnumSet.class : LinkedHashSet.class;
		Preconditions.check(targetType.isAssignableFrom(targetInstance));

		if (!isEnum) {
			return deserializeSimpleSet(targetType, version, serializerCaller);
		} else {
			return deserializeEnumSet(targetType, version, serializerCaller);
		}
	}

	private FunctionDef deserializeEnumSet(Class<?> targetType, final int version, final SerializerStaticCaller serializerCaller) {
		FunctionDef len = let(call(arg(0), "readVarInt"));
		FunctionDef container = let(newArray(Object[].class, len));
		FunctionDef array = arrayForEachWithChanges(container, new ForEachWithChanges() {
			@Override
			public FunctionDef forEachWithChanges() {
				return serializerCaller.deserialize(valueSerializer, version, valueSerializer.getRawType());
			}
		});
		FunctionDef list = let(cast(callStatic(Arrays.class, "asList", container), Collection.class));
		FunctionDef enumSet = callStatic(EnumSet.class, "copyOf", list);
		return sequence(len, container, array, list, enumSet);
	}

	private FunctionDef deserializeSimpleSet(Class<?> targetType, final int version, final SerializerStaticCaller serializerCaller) {
		FunctionDef length = let(call(arg(0), "readVarInt"));
		final FunctionDef container = let(constructor(LinkedHashSet.class, length));
		return sequence(length, container, functionFor(length, new ForVar() {
					@Override
					public FunctionDef forVar(FunctionDef local) {
						return sequence(
								call(container, "add", cast(serializerCaller.deserialize(valueSerializer, version, valueSerializer.getRawType()), Object.class)),
								voidFunc()
						);
					}
				}), container
		);
	}
}
