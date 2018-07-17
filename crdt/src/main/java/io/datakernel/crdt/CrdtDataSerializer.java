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

package io.datakernel.crdt;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serializer.BufferSerializer;

public final class CrdtDataSerializer<K extends Comparable<K>, S> implements BufferSerializer<CrdtData<K, S>> {
	private final BufferSerializer<K> keySerializer;
	private final BufferSerializer<S> stateSerializer;

	public CrdtDataSerializer(BufferSerializer<K> keySerializer, BufferSerializer<S> stateSerializer) {
		this.keySerializer = keySerializer;
		this.stateSerializer = stateSerializer;
	}

	public BufferSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public BufferSerializer<S> getStateSerializer() {
		return stateSerializer;
	}

	@Override
	public void serialize(ByteBuf output, CrdtData<K, S> item) {
		keySerializer.serialize(output, item.getKey());
		stateSerializer.serialize(output, item.getState());
	}

	@Override
	public CrdtData<K, S> deserialize(ByteBuf input) {
		return new CrdtData<>(keySerializer.deserialize(input), stateSerializer.deserialize(input));
	}
}
