package io.global.common;

import java.math.BigInteger;
import java.security.SecureRandom;

import static io.global.common.CryptoUtils.SECURE_RANDOM;

public final class PrivKey {
	public static final SecureRandom SECURE_RANDOM = new SecureRandom();

	private final BigInteger privateKeyForSigning;

	public PrivKey(BigInteger privateKeyForSigning) {
		this.privateKeyForSigning = privateKeyForSigning;
	}

	public BigInteger getPrivateKeyForSigning() {
		return privateKeyForSigning;
	}

	public static PrivKey random() {
		byte[] privKeyBytes = new byte[32];
		SECURE_RANDOM.nextBytes(privKeyBytes);
		BigInteger privKey = new BigInteger(1, privKeyBytes);
		return new PrivKey(privKey);
	}
}
