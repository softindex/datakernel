package io.global.common;

import org.spongycastle.crypto.params.ECPublicKeyParameters;
import org.spongycastle.math.ec.ECPoint;

public final class PubKey {
	private final ECPublicKeyParameters ecPublicKey;

	private PubKey(ECPublicKeyParameters ecPublicKey) {
		this.ecPublicKey = ecPublicKey;
	}

	public static PubKey ofQ(ECPoint q) {
		return new PubKey(new ECPublicKeyParameters(q, CryptoUtils.CURVE));
	}

	public ECPublicKeyParameters getEcPublicKey() {
		return ecPublicKey;
	}

}
