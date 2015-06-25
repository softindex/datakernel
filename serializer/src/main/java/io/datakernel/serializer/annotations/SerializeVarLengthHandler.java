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
import io.datakernel.serializer.asm.SerializerGenInt;
import io.datakernel.serializer.asm.SerializerGenLong;

import static com.google.common.base.Preconditions.checkArgument;

public final class SerializeVarLengthHandler implements AnnotationHandler<SerializeVarLength, SerializeVarLengthEx> {
	@Override
	public SerializerGenBuilder createBuilder(SerializerScanner serializerScanner, SerializeVarLength annotation) {
//        final boolean zigzag = annotation.zigzag();
		return new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, SerializerForType[] generics, SerializerGen fallback) {
				checkArgument(generics.length == 0);
				if (type == Integer.TYPE) {
					return new SerializerGenInt(true);
				}
				if (type == Integer.class) {
					return new SerializerGenInt(true);
				}
				if (type == Long.TYPE) {
					return new SerializerGenLong(true);
				}
				if (type == Long.class) {
					return new SerializerGenLong(true);
				}
				throw new IllegalArgumentException();
			}
		};
	}

	@Override
	public int[] extractPath(SerializeVarLength annotation) {
		return annotation.path();
	}

	@Override
	public SerializeVarLength[] extractList(SerializeVarLengthEx plural) {
		return plural.value();
	}
}
