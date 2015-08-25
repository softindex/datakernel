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

import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer2.SerializerScanner;
import io.datakernel.serializer2.asm.SerializerGenNullable;


public final class SerializeNullableHandler implements AnnotationHandler<SerializeNullable, SerializeNullableEx> {
	@Override
	public io.datakernel.serializer2.asm.SerializerGenBuilder createBuilder(SerializerScanner serializerScanner, SerializeNullable annotation) {
		return new io.datakernel.serializer2.asm.SerializerGenBuilder() {
			@Override
			public io.datakernel.serializer2.asm.SerializerGen serializer(Class<?> type, SerializerForType[] generics, io.datakernel.serializer2.asm.SerializerGen fallback) {
				Preconditions.check(!type.isPrimitive());
				if (fallback instanceof io.datakernel.serializer2.asm.SerializerGenString)
					return ((io.datakernel.serializer2.asm.SerializerGenString) fallback).nullable(true);
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
