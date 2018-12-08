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

package io.datakernel.serializer.asm;

import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class SerializerGenBuilderConst implements SerializerGenBuilder {
	private final SerializerGen serializer;

	public SerializerGenBuilderConst(SerializerGen serializer) {
		this.serializer = checkNotNull(serializer);
	}

	@Override
	public SerializerGen serializer(Class<?> type, SerializerForType[] generics, SerializerGen fallback) {
		check(generics.length == 0, "Type should have no generics");
		return serializer;
	}
}
