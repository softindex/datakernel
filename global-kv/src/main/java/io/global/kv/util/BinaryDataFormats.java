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

package io.global.kv.util;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.registry.CodecFactory;
import io.global.common.Hash;
import io.global.kv.api.KvItem;
import io.global.kv.api.RawKvItem;

import static io.datakernel.codec.StructuredCodecs.LONG_CODEC;
import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.global.common.BinaryDataFormats.createGlobal;

public final class BinaryDataFormats {
	private BinaryDataFormats() {
		throw new AssertionError("nope.");
	}

	public static final CodecFactory REGISTRY = createGlobal()
			.with(RawKvItem.class, registry ->
					tuple(RawKvItem::parse,
							RawKvItem::getKey, registry.get(byte[].class),
							RawKvItem::getValue, registry.get(byte[].class).nullable(),
							RawKvItem::getTimestamp, registry.get(long.class),
							RawKvItem::getSimKeyHash, registry.get(Hash.class).nullable()))

			.withGeneric(KvItem.class, (registry, subCodecs) ->
					tuple(KvItem::new,
							KvItem::getTimestamp, LONG_CODEC,
							KvItem::getKey, subCodecs[0],
							KvItem::getValue, subCodecs[1]));

	public static final StructuredCodec<RawKvItem> RAW_KV_ITEM_CODEC = REGISTRY.get(RawKvItem.class);
}
