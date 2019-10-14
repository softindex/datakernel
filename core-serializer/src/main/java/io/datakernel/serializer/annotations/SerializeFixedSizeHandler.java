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
import io.datakernel.serializer.HasFixedSize;
import io.datakernel.serializer.SerializerBuilder.Helper;
import io.datakernel.serializer.asm.SerializerGenBuilder;

public final class SerializeFixedSizeHandler implements AnnotationHandler<SerializeFixedSize, SerializeFixedSizeEx> {
	@Override
	public SerializerGenBuilder createBuilder(Helper serializerBuilder, SerializeFixedSize annotation, CompatibilityLevel compatibilityLevel) {
		return (type, generics, target) -> {
			if (target instanceof HasFixedSize) {
				return ((HasFixedSize) target).withFixedSize(annotation.value());
			}
			throw new IllegalArgumentException("Unsupported serializer " + target);
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
