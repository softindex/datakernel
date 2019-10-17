package io.global.pm.util;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.writer.ByteBufWriter;
import io.global.common.PubKey;
import io.global.pm.api.Message;

import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static java.nio.charset.StandardCharsets.UTF_8;

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

	public static <T> ByteBufsParser<T> ndJsonParser(StructuredCodec<T> codec) {
		return ByteBufsParser.ofLfTerminatedBytes()
				.andThen(value -> JsonUtils.fromJson(codec, value.asString(UTF_8)));
	}

	public static <T> ByteBuf toNdJsonBuf(StructuredCodec<T> codec, T value) {
		ByteBufWriter writer = new ByteBufWriter();
		toJson(codec, value, writer);
		writer.write(LF);
		return writer.getBuf();
	}
}
