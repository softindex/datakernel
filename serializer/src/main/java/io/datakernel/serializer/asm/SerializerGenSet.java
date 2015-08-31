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

import io.datakernel.codegen.ForEachWithChanges;
import io.datakernel.codegen.ForVar;
import io.datakernel.codegen.FunctionDef;
import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer.SerializerFactory;

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
	public void prepareSerializeStaticMethods(int version, SerializerFactory.StaticMethods staticMethods) {
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods);
	}

	@Override
	public FunctionDef serialize(FunctionDef value, final int version, final SerializerFactory.StaticMethods staticMethods) {
		return sequence(
				call(arg(0), "writeVarInt", call(value, "size")),
				setForEach(value, new ForVar() {
					@Override
					public FunctionDef forVar(FunctionDef local) {
						return sequence(valueSerializer.serialize(cast(local, valueSerializer.getRawType()), version, staticMethods), voidFunc());
					}
				})
		);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerFactory.StaticMethods staticMethods) {
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods);
	}

	@Override
	public FunctionDef deserialize(Class<?> targetType, int version, SerializerFactory.StaticMethods staticMethods) {
		boolean isEnum = valueSerializer.getRawType().isEnum();
		Class<?> targetInstance = isEnum ? EnumSet.class : LinkedHashSet.class;
		Preconditions.check(targetType.isAssignableFrom(targetInstance));

		if (!isEnum) {
			return deserializeSimpleSet(version, staticMethods);
		} else {
			return deserializeEnumSet(version, staticMethods);
		}
	}

	private FunctionDef deserializeEnumSet(final int version, final SerializerFactory.StaticMethods staticMethods) {
		FunctionDef len = let(call(arg(0), "readVarInt"));
		FunctionDef container = let(newArray(Object[].class, len));
		FunctionDef array = arrayForEachWithChanges(container, new ForEachWithChanges() {
			@Override
			public FunctionDef forEachWithChanges() {
				return valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods);
			}
		});
		FunctionDef list = let(cast(callStatic(Arrays.class, "asList", container), Collection.class));
		FunctionDef enumSet = callStatic(EnumSet.class, "copyOf", list);
		return sequence(len, container, array, list, enumSet);
	}

	private FunctionDef deserializeSimpleSet(final int version, final SerializerFactory.StaticMethods staticMethods) {
		FunctionDef length = let(call(arg(0), "readVarInt"));
		final FunctionDef container = let(constructor(LinkedHashSet.class, length));
		return sequence(length, container, functionFor(length, new ForVar() {
					@Override
					public FunctionDef forVar(FunctionDef local) {
						return sequence(
								call(container, "add", cast(valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods), Object.class)),
								voidFunc()
						);
					}
				}), container
		);
	}
}
