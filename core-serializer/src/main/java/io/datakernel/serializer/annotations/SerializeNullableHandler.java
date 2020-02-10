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
import io.datakernel.serializer.impl.SerializerDefBuilder;
import io.datakernel.serializer.impl.SerializerDefNullable;
import io.datakernel.serializer.impl.SerializerDefString;
import io.datakernel.serializer.impl.SerializerDefWithNullable;

import static io.datakernel.common.Preconditions.checkArgument;
import static io.datakernel.serializer.CompatibilityLevel.LEVEL_3;

public final class SerializeNullableHandler implements AnnotationHandler<SerializeNullable, SerializeNullableEx> {
	@Override
	public SerializerDefBuilder createBuilder(Helper serializerBuilder, SerializeNullable annotation, CompatibilityLevel compatibilityLevel) {
		return (type, generics, target) -> {
			checkArgument(!type.isPrimitive(), "Type must not represent a primitive type");
			if (target instanceof SerializerDefString)
				return ((SerializerDefString) target).ensureNullable();
			if (compatibilityLevel.compareTo(LEVEL_3) >= 0 && target instanceof SerializerDefWithNullable) {
				return ((SerializerDefWithNullable) target).ensureNullable();
			}
			return new SerializerDefNullable(target);
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
