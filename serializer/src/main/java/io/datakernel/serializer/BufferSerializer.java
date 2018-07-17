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

package io.datakernel.serializer;

import io.datakernel.bytebuf.ByteBuf;

import java.util.function.Function;

public interface BufferSerializer<T> {

	void serialize(ByteBuf output, T item);

	T deserialize(ByteBuf input);

	default <U> BufferSerializer<U> transform(Function<U, T> from, Function<T, U> into) {
		return new BufferSerializer<U>() {
			@Override
			public void serialize(ByteBuf output, U item) {
				BufferSerializer.this.serialize(output, from.apply(item));
			}

			@Override
			public U deserialize(ByteBuf input) {
				return into.apply(BufferSerializer.this.deserialize(input));
			}
		};
	}
}
