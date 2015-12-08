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

package io.datakernel.serializer.annotations;

import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.asm.*;

public final class SerializeNullableHandler implements AnnotationHandler<SerializeNullable, SerializeNullableEx> {
	@Override
	public SerializerGenBuilder createBuilder(SerializerBuilder serializerBuilder, SerializeNullable annotation, final CompatibilityLevel compatibilityLevel) {
		return new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, SerializerForType[] generics, SerializerGen fallback) {
				Preconditions.check(!type.isPrimitive());
				if (fallback instanceof SerializerGenString)
					return ((SerializerGenString) fallback).nullable(true);
				if (fallback instanceof SerializerGenEnum && compatibilityLevel == CompatibilityLevel.LEVEL_3)
					return (((SerializerGenEnum) fallback)).nullable(true);
				if (fallback instanceof SerializerGenSubclass && compatibilityLevel == CompatibilityLevel.LEVEL_3)
					return ((SerializerGenSubclass) fallback).nullable(true);
				return new SerializerGenNullable(fallback);
			}
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
