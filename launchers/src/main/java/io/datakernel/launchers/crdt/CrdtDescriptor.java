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

package io.datakernel.launchers.crdt;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.crdt.CrdtDataSerializer;

import java.util.function.BinaryOperator;

@Singleton
public final class CrdtDescriptor<K extends Comparable<K>, S> {
	private final BinaryOperator<S> combiner;
	private final CrdtDataSerializer<K, S> serializer;
	private final StructuredCodec<K> keyCodec;
	private final StructuredCodec<S> stateCodec;

	@Inject
	public CrdtDescriptor(BinaryOperator<S> combiner, CrdtDataSerializer<K, S> serializer, StructuredCodec<K> keyCodec, StructuredCodec<S> stateCodec) {
		this.combiner = combiner;
		this.serializer = serializer;
		this.keyCodec = keyCodec;
		this.stateCodec = stateCodec;
	}

	public BinaryOperator<S> getCombiner() {
		return combiner;
	}

	public CrdtDataSerializer<K, S> getSerializer() {
		return serializer;
	}

	public StructuredCodec<K> getKeyCodec() {
		return keyCodec;
	}

	public StructuredCodec<S> getStateCodec() {
		return stateCodec;
	}
}
