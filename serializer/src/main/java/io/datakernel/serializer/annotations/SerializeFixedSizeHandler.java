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

import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.asm.SerializerGenArray;
import io.datakernel.serializer.asm.SerializerGenBuilder;
import io.datakernel.serializer.asm.SerializerGenList;

public final class SerializeFixedSizeHandler implements AnnotationHandler<SerializeFixedSize, SerializeFixedSizeEx> {
	@Override
	public SerializerGenBuilder createBuilder(SerializerBuilder serializerBuilder, final SerializeFixedSize annotation, CompatibilityLevel compatibilityLevel) {
		return new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, SerializerForType[] generics, SerializerGen fallback) {
				if (fallback instanceof SerializerGenArray) {
					return ((SerializerGenArray) fallback).fixedSize(annotation.value(), type);
				}
				if (fallback instanceof SerializerGenList) {
					throw new UnsupportedOperationException();
				}
				throw new IllegalArgumentException();
			}
		};
	}

	@Override
	public int[] extractPath(SerializeFixedSize annotation) {
		return annotation.path();
	}

	@Override
	public SerializeFixedSize[] extractList(SerializeFixedSizeEx plural) {
		return plural.value();
	}
}
