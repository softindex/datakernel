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

package io.global.db.util;

import io.datakernel.codec.registry.CodecFactory;
import io.global.common.Hash;
import io.global.db.Blob;
import io.global.db.DbItem;

import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.global.common.BinaryDataFormats.createGlobal;

public final class BinaryDataFormats {
	private BinaryDataFormats() {
		throw new AssertionError("nope.");
	}

	public static final CodecFactory REGISTRY = createGlobal()
			.with(Blob.class, registry ->
					tuple(Blob::parse,
							Blob::getTimestamp, registry.get(long.class),
							Blob::getData, registry.get(byte[].class).nullable()))

			.with(DbItem.class, registry ->
					tuple(DbItem::parse,
							DbItem::getKey, registry.get(byte[].class),
							DbItem::getValue, registry.get(Blob.class),
							DbItem::getSimKeyHash, registry.get(Hash.class).nullable()));
}
