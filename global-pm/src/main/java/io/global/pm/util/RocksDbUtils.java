package io.global.pm.util;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.common.tuple.Tuple4;
import io.global.common.PubKey;
import io.global.common.Signature;
import io.global.common.SignedData;
import io.global.pm.api.RawMessage;

import static io.datakernel.codec.StructuredCodecs.LONG64_CODEC;
import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.global.pm.util.BinaryDataFormats.*;

public final class RocksDbUtils {
	private static final StructuredCodec<Tuple2<PubKey, String>> MAILBOX_PREFIX_CODEC = REGISTRY.get(new TypeT<Tuple2<PubKey, String>>() {});
	private static final StructuredCodec<Tuple4<PubKey, String, Long, Long>> KEY_CODEC = tuple(Tuple4::new,
			Tuple4::getValue1, REGISTRY.get(PubKey.class),
			Tuple4::getValue2, REGISTRY.get(String.class),
			Tuple4::getValue3, LONG64_CODEC,
			Tuple4::getValue4, REGISTRY.get(Long.class));
	private static final StructuredCodec<Tuple2<Signature, byte[]>> VALUE_CODEC = tuple(Tuple2::new,
			Tuple2::getValue1, REGISTRY.get(Signature.class),
			Tuple2::getValue2, REGISTRY.get(byte[].class).nullable());

	public static byte[] prefixedBySpace(PubKey space) {
		return encodeAsArray(PUB_KEY_CODEC, space);
	}

	public static byte[] prefixedByMailBox(PubKey space, String mailBox) {
		return encodeAsArray(MAILBOX_PREFIX_CODEC, new Tuple2<>(space, mailBox));
	}

	public static byte[] prefixedByTimestamp(PubKey space, String mailBox, long timestamp) {
		return encodeAsArray(KEY_CODEC, new Tuple4<>(space, mailBox, timestamp, Long.MIN_VALUE));
	}

	public static Tuple2<byte[], byte[]> pack(PubKey space, String mailBox, SignedData<RawMessage> message) {
		RawMessage rawMessage = message.getValue();
		Signature signature = message.getSignature();
		byte[] key = encodeAsArray(KEY_CODEC, new Tuple4<>(space, mailBox, rawMessage.getTimestamp(), rawMessage.getId()));
		byte[] value = encodeAsArray(VALUE_CODEC, new Tuple2<>(signature, rawMessage.isTombstone() ? null : rawMessage.getEncrypted()));
		return new Tuple2<>(key, value);
	}

	public static SignedData<RawMessage> unpack(byte[] key, byte[] value) throws ParseException {
		Tuple4<PubKey, String, Long, Long> keyTuple = unpackKey(key);
		Tuple2<Signature, byte[]> valueTuple = decode(VALUE_CODEC, value);
		RawMessage rawMessage = RawMessage.parse(keyTuple.getValue4(), keyTuple.getValue3(), valueTuple.getValue2());
		return SignedData.parse(RAW_MESSAGE_CODEC, rawMessage, valueTuple.getValue1());
	}

	public static Tuple4<PubKey, String, Long, Long> unpackKey(byte[] key) throws ParseException {
		return decode(KEY_CODEC, key);
	}

}
