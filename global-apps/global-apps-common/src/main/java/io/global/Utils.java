package io.global;

import io.datakernel.codec.StructuredCodec;
import io.global.common.PrivKey;
import io.global.common.PubKey;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.global.common.CryptoUtils.randomBytes;
import static io.global.common.CryptoUtils.toHexString;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final StructuredCodec<PubKey> PUB_KEY_HEX_CODEC = STRING_CODEC.transform(PubKey::fromString, PubKey::asString);
	public static final StructuredCodec<PrivKey> PRIV_KEY_HEX_CODEC = STRING_CODEC.transform(PrivKey::fromString, PrivKey::asString);

	public static String generateString(int size) {
		return toHexString(randomBytes(size));

	}
}
