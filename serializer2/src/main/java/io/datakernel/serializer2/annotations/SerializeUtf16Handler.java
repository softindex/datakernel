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

package io.datakernel.serializer2.annotations;

public final class SerializeUtf16Handler implements AnnotationHandler<SerializeUtf16, SerializeUtf16Ex> {
	@Override
	public io.datakernel.serializer2.asm.SerializerGenBuilder createBuilder(io.datakernel.serializer2.SerializerScanner serializerScanner, SerializeUtf16 annotation) {
		return new io.datakernel.serializer2.asm.SerializerGenBuilder() {
			@SuppressWarnings("ConstantConditions")
			@Override
			public io.datakernel.serializer2.asm.SerializerGen serializer(Class<?> type, SerializerForType[] generics, io.datakernel.serializer2.asm.SerializerGen fallback) {
				io.datakernel.serializer2.asm.SerializerGenString gen = (io.datakernel.serializer2.asm.SerializerGenString) fallback;
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
