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

import io.datakernel.codec.StructuredCodec;
import io.datakernel.crdt.CrdtData.CrdtDataSerializer;
import io.datakernel.crdt.CrdtOperator;

public final class CrdtDescriptor<K extends Comparable<K>, S> {
	private final CrdtOperator<S> crdtOperator;
	private final CrdtDataSerializer<K, S> serializer;
	private final StructuredCodec<K> keyCodec;
	private final StructuredCodec<S> stateCodec;

	public CrdtDescriptor(CrdtOperator<S> crdtOperator, CrdtDataSerializer<K, S> serializer, StructuredCodec<K> keyCodec, StructuredCodec<S> stateCodec) {
		this.crdtOperator = crdtOperator;
		this.serializer = serializer;
		this.keyCodec = keyCodec;
		this.stateCodec = stateCodec;
	}

	public CrdtOperator<S> getCrdtOperator() {
		return crdtOperator;
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
