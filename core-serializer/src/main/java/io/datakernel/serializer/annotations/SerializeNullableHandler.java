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

package io.datakernel.serializer.annotations;

import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.SerializerBuilder.Helper;
import io.datakernel.serializer.asm.SerializerGenBuilder;
import io.datakernel.serializer.asm.SerializerGenNullable;
import io.datakernel.serializer.asm.SerializerGenString;

import static io.datakernel.common.Preconditions.checkArgument;

public final class SerializeNullableHandler implements AnnotationHandler<SerializeNullable, SerializeNullableEx> {
	@Override
	public SerializerGenBuilder createBuilder(Helper serializerBuilder, SerializeNullable annotation, CompatibilityLevel compatibilityLevel) {
		return (type, generics, fallback) -> {
			checkArgument(!type.isPrimitive(), "Type must not represent a primitive type");
			if (fallback instanceof SerializerGenString)
				return ((SerializerGenString) fallback).nullable(true);
			if (compatibilityLevel == CompatibilityLevel.LEVEL_3 && fallback instanceof NullableOptimization) {
				return ((NullableOptimization) fallback).asNullable();
			}
			return new SerializerGenNullable(fallback);
		};
	}

	@Override
	public int[] extractPath(SerializeNullable annotation) {
		return annotation.path();
	}

	@Override
	public SerializeNullable[] extractList(SerializeNullableEx plural) {
		return plural.value();
	}
}
