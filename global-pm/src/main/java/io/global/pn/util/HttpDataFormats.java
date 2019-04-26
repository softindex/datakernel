package io.global.pn.util;

import io.datakernel.codec.StructuredCodec;
import io.global.common.PubKey;
import io.global.pn.api.Message;

import static io.datakernel.codec.StructuredCodecs.*;

public final class HttpDataFormats {
	private HttpDataFormats() {
		throw new AssertionError();
	}

	public static final StructuredCodec<PubKey> PUB_KEY_HEX_CODEC = STRING_CODEC.transform(PubKey::fromString, PubKey::asString);

	public static <T> StructuredCodec<Message<T>> getMessageCodec(StructuredCodec<T> payloadCodec) {
		return object(Message::parse,
				"id", Message::getId, LONG_CODEC,
				"timestamp", Message::getTimestamp, LONG_CODEC,
				"sender", Message::getSender, PUB_KEY_HEX_CODEC,
				"payload", Message::getPayload, payloadCodec);
	}

}
