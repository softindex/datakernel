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

package io.global.crdt;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.crdt.CrdtData;
import io.global.common.Hash;

import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.global.common.BinaryDataFormats.createGlobal;

public final class BinaryDataFormats {
	private BinaryDataFormats() {}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public static final CodecFactory REGISTRY = createGlobal()
			.with(RawCrdtData.class, registry ->
					tuple(RawCrdtData::parse,
							RawCrdtData::getKey, registry.get(byte[].class),
							RawCrdtData::getValue, registry.get(byte[].class),
							RawCrdtData::getSimKeyHash, registry.get(Hash.class).nullable()))

			.withGeneric(CrdtData.class, (registry, subCodecs) ->
					tuple(CrdtData::new,
							CrdtData::getKey, (StructuredCodec<Comparable>) (StructuredCodec) subCodecs[0],
							CrdtData::getState, subCodecs[1]));

	public static final StructuredCodec<RawCrdtData> RAW_CRDT_DATA_CODEC = REGISTRY.get(RawCrdtData.class);
	public static final StructuredCodec<byte[]> BYTES_CODEC = REGISTRY.get(byte[].class);
}
