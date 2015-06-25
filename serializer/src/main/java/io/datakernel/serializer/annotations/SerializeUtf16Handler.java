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

import io.datakernel.serializer.SerializerScanner;
import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.asm.SerializerGenBuilder;
import io.datakernel.serializer.asm.SerializerGenString;

public final class SerializeUtf16Handler implements AnnotationHandler<SerializeUtf16, SerializeUtf16Ex> {
	@Override
	public SerializerGenBuilder createBuilder(SerializerScanner serializerScanner, SerializeUtf16 annotation) {
		return new SerializerGenBuilder() {
			@SuppressWarnings("ConstantConditions")
			@Override
			public SerializerGen serializer(Class<?> type, SerializerForType[] generics, SerializerGen fallback) {
				SerializerGenString gen = (SerializerGenString) fallback;
				return gen.utf16(true);
			}
		};
	}

	@Override
	public int[] extractPath(SerializeUtf16 annotation) {
		return annotation.path();
	}

	@Override
	public SerializeUtf16[] extractList(SerializeUtf16Ex plural) {
		return plural.value();
	}
}
