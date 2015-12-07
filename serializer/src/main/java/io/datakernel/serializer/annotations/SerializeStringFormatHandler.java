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
import io.datakernel.serializer.StringFormat;
import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.asm.SerializerGenBuilder;
import io.datakernel.serializer.asm.SerializerGenString;

public class SerializeStringFormatHandler implements AnnotationHandler<SerializeStringFormat, SerializeStringFormatEx> {
	@Override
	public SerializerGenBuilder createBuilder(SerializerBuilder serializerBuilder, final SerializeStringFormat annotation, final CompatibilityLevel compatibilityLevel) {
		return new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, SerializerForType[] generics, SerializerGen fallback) {
				if (compatibilityLevel == CompatibilityLevel.VERSION_1) {
					if (annotation.value() == StringFormat.ISO_8859_1 || annotation.value() == StringFormat.UTF8) {
						return ((SerializerGenString) fallback).encoding(StringFormat.UTF8_CUSTOM);
					}
				}
				return ((SerializerGenString) fallback).encoding(annotation.value());
			}
		};
	}

	@Override
	public int[] extractPath(SerializeStringFormat annotation) {
		return annotation.path();
	}

	@Override
	public SerializeStringFormat[] extractList(SerializeStringFormatEx plural) {
		return plural.value();
	}
}
