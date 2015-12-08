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
import io.datakernel.codegen.Variable;
import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.SerializerUtils;

import java.util.*;

import static io.datakernel.codegen.Expressions.*;

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
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression serialize(Expression byteArray, final Variable off, Expression value, final int version, final SerializerBuilder.StaticMethods staticMethods, final CompatibilityLevel compatibilityLevel) {
		return sequence(
				set(off, callStatic(SerializerUtils.class, "writeVarInt", byteArray, off, call(value, "size"))),
				forEach(value, valueSerializer.getRawType(), new ForVar() {
					@Override
					public Expression forVar(Expression it) {
						return set(off, valueSerializer.serialize(arg(0), arg(1), it, version, staticMethods, compatibilityLevel));
					}
				}),
				off
		);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		boolean isEnum = valueSerializer.getRawType().isEnum();
		Class<?> targetInstance = isEnum ? EnumSet.class : LinkedHashSet.class;
		Preconditions.check(targetType.isAssignableFrom(targetInstance));

		if (!isEnum) {
			return deserializeSimpleSet(version, staticMethods, compatibilityLevel);
		} else {
			return deserializeEnumSet(version, staticMethods, compatibilityLevel);
		}
	}

	private Expression deserializeEnumSet(final int version,
	                                      final SerializerBuilder.StaticMethods staticMethods,
	                                      final CompatibilityLevel compatibilityLevel) {
		Expression len = let(call(arg(0), "readVarInt"));
		final Expression container = let(newArray(Object[].class, len));
		Expression array = expressionFor(len, new ForVar() {
			@Override
			public Expression forVar(Expression it) {
				return setArrayItem(container, it, valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods, compatibilityLevel));
			}
		});
		Expression list = let(cast(callStatic(Arrays.class, "asList", container), Collection.class));
		Expression enumSet = callStatic(EnumSet.class, "copyOf", list);
		return sequence(len, container, array, list, enumSet);
	}

	private Expression deserializeSimpleSet(final int version,
	                                        final SerializerBuilder.StaticMethods staticMethods,
	                                        final CompatibilityLevel compatibilityLevel) {
		Expression length = let(call(arg(0), "readVarInt"));
		final Expression container = let(constructor(LinkedHashSet.class, length));
		return sequence(length, container, expressionFor(length, new ForVar() {
					@Override
					public Expression forVar(Expression it) {
						return sequence(
								call(container, "add", cast(valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods, compatibilityLevel), Object.class)),
								voidExp()
						);
					}
				}), container
		);
	}
}
