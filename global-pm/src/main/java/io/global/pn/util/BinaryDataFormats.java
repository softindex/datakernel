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

package io.global.pn.util;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.util.TypeT;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pn.api.RawMessage;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.codec.StructuredCodecs.BYTES_CODEC;
import static io.global.common.BinaryDataFormats.createGlobal;

public final class BinaryDataFormats {
	private BinaryDataFormats() {
		throw new AssertionError("nope.");
	}

	public static final CodecFactory REGISTRY = createGlobal()
			.with(RawMessage.class, tuple(RawMessage::new,
					RawMessage::getId, LONG64_CODEC,
					RawMessage::getTimestamp, LONG_CODEC,
					RawMessage::getEncrypted, BYTES_CODEC));

	public static final StructuredCodec<SignedData<RawMessage>> SIGNED_RAW_MSG_CODEC = REGISTRY.get(new TypeT<SignedData<RawMessage>>() {});
	public static final StructuredCodec<RawMessage> RAW_MESSAGE_CODEC = REGISTRY.get(RawMessage.class);
	public static final StructuredCodec<PubKey> PUB_KEY_CODEC = REGISTRY.get(PubKey.class);
}
