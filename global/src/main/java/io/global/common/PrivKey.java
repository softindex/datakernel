package io.global.common;

import java.math.BigInteger;

public final class PrivKey {
	private final BigInteger privateKeyForSigning;

	public PrivKey(BigInteger privateKeyForSigning) {
		this.privateKeyForSigning = privateKeyForSigning;
	}

	public BigInteger getPrivateKeyForSigning() {
		return privateKeyForSigning;
	}
}
