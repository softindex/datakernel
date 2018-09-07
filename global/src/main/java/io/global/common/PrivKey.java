package io.global.common;

import org.spongycastle.crypto.params.ECPrivateKeyParameters;

import java.math.BigInteger;

public final class PrivKey {
	private final ECPrivateKeyParameters ecPrivateKey;

	public PrivKey(ECPrivateKeyParameters ecPrivateKey) {
		this.ecPrivateKey = ecPrivateKey;
	}

	public static PrivKey ofD(BigInteger d) {
		return new PrivKey(new ECPrivateKeyParameters(d, CryptoUtils.CURVE));
	}

	public PubKey computePubKey() {
		return new PubKey(CryptoUtils.computePubKey(ecPrivateKey));
	}

	public ECPrivateKeyParameters getEcPrivateKey() {
		return ecPrivateKey;
	}
}
