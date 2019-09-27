package io.global.pm;

import io.datakernel.codec.StructuredCodec;

import static io.datakernel.codec.StructuredCodecs.LONG_CODEC;
import static io.datakernel.codec.StructuredCodecs.object;
import static io.global.Utils.PUB_KEY_HEX_CODEC;

public final class PmUtils {
	public static <K, V> StructuredCodec<Message<K, V>> getMessageCodec(StructuredCodec<K> idCodec, StructuredCodec<V> payloadCodec) {
		return object(Message::parse,
				"id", Message::getId, idCodec,
				"timestamp", Message::getTimestamp, LONG_CODEC,
				"sender", Message::getSender, PUB_KEY_HEX_CODEC,
				"payload", Message::getPayload, payloadCodec);
	}
}
