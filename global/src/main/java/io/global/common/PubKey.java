package io.global.common;

import org.spongycastle.crypto.params.ECPublicKeyParameters;
import org.spongycastle.math.ec.ECPoint;
import org.spongycastle.math.ec.FixedPointCombMultiplier;

import static io.global.common.CryptoUtils.CURVE;

public final class PubKey {
	private final ECPublicKeyParameters ecPublicKey;

	private PubKey(ECPublicKeyParameters ecPublicKey) {
		this.ecPublicKey = ecPublicKey;
	}

	public static PubKey ofQ(ECPoint q) {
		return new PubKey(new ECPublicKeyParameters(q, CryptoUtils.CURVE));
	}

	public static PubKey ofPrivKey(PrivKey privKey) {
		return ofQ(new FixedPointCombMultiplier().multiply(CURVE.getG(), privKey.getPrivateKeyForSigning()));
	}

	public ECPublicKeyParameters getEcPublicKey() {
		return ecPublicKey;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PubKey pubKey = (PubKey) o;
		return ecPublicKey.getQ().equals(pubKey.ecPublicKey.getQ());
	}

	@Override
	public int hashCode() {
		return ecPublicKey.getQ().hashCode();
	}
}
