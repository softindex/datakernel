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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		PrivKey privKey = (PrivKey) o;

		return ecPrivateKey.getD().equals(privKey.ecPrivateKey.getD());
	}

	@Override
	public int hashCode() {
		return ecPrivateKey.getD().hashCode();
	}

	@Override
	public String toString() {
		return "PrivKey{}";
	}
}
