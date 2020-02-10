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
import io.datakernel.serializer.SerializerBuilder.Helper;
import io.datakernel.serializer.StringFormat;
import io.datakernel.serializer.impl.SerializerDefBuilder;
import io.datakernel.serializer.impl.SerializerDefString;

public class SerializeStringFormatHandler implements AnnotationHandler<SerializeStringFormat, SerializeStringFormatEx> {
	@SuppressWarnings("deprecation") // compatibility
	@Override
	public SerializerDefBuilder createBuilder(Helper serializerBuilder, SerializeStringFormat annotation, CompatibilityLevel compatibilityLevel) {
		return (type, generics, target) -> {
			if (compatibilityLevel == CompatibilityLevel.LEVEL_1) {
				if (annotation.value() == StringFormat.ISO_8859_1 || annotation.value() == StringFormat.UTF8) {
					return ((SerializerDefString) target).encoding(StringFormat.UTF8_MB3);
				}
			}
			return ((SerializerDefString) target).encoding(annotation.value());
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
