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

package io.global.pm.util;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.util.TypeT;
import io.global.common.PubKey;
import io.global.common.Signature;
import io.global.common.SignedData;
import io.global.pm.api.RawMessage;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.global.common.BinaryDataFormats.createGlobal;

public final class BinaryDataFormats {
	private BinaryDataFormats() {
		throw new AssertionError("nope.");
	}

	public static final CodecFactory REGISTRY = createGlobal()
			.with(RawMessage.class, tuple(RawMessage::of,
					RawMessage::getId, LONG_CODEC,
					RawMessage::getTimestamp, LONG_CODEC,
					rawMessage -> rawMessage.isTombstone() ? null : rawMessage.getEncrypted(), BYTES_CODEC.nullable()));

	public static final StructuredCodec<SignedData<RawMessage>> SIGNED_RAW_MSG_CODEC = REGISTRY.get(new TypeT<SignedData<RawMessage>>() {});
	public static final StructuredCodec<SignedData<Long>> SIGNED_LONG_CODEC =
			tuple((bytes, signature) -> SignedData.parse(LONG_CODEC, bytes, signature),
					SignedData::getBytes, REGISTRY.get(byte[].class),
					SignedData::getSignature, REGISTRY.get(Signature.class));
	public static final StructuredCodec<RawMessage> RAW_MESSAGE_CODEC = REGISTRY.get(RawMessage.class);
	public static final StructuredCodec<PubKey> PUB_KEY_CODEC = REGISTRY.get(PubKey.class);

	public static final ByteBufsParser<SignedData<RawMessage>> SIGNED_RAW_MSG_PARSER = ByteBufsParser.ofDecoder(SIGNED_RAW_MSG_CODEC);
	public static final ByteBufsParser<SignedData<Long>> SIGNED_LONG_PARSER = ByteBufsParser.ofDecoder(SIGNED_LONG_CODEC);
}
