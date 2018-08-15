package io.global.common;

import org.spongycastle.crypto.CryptoException;
import org.spongycastle.crypto.params.KeyParameter;

import java.security.SecureRandom;

import static io.global.common.CryptoUtils.decryptECIES;

public final class SimKey {
	public static final SecureRandom SECURE_RANDOM = new SecureRandom();

	private final KeyParameter keyParameter;

	private SimKey(KeyParameter keyParameter) {
		this.keyParameter = keyParameter;
	}

	public static SimKey random() {
		byte[] aesKeyBytes = new byte[16];
		SECURE_RANDOM.nextBytes(aesKeyBytes);
		KeyParameter aesKey = new KeyParameter(aesKeyBytes);
		return ofAesKey(aesKey);
	}

	public static SimKey ofAesKey(KeyParameter keyParameter) {
		return new SimKey(keyParameter);
	}

	public static SimKey ofBytes(byte[] bytes) {
		return new SimKey(new KeyParameter(bytes));
	}

	public static SimKey ofEncryptedSimKey(EncryptedSimKey encryptedSimKey, PrivKey privKey) {
		try {
			return SimKey.ofBytes(decryptECIES(encryptedSimKey.toBytes(), privKey.getPrivateKeyForSigning()));
		} catch (CryptoException e) {
			throw new RuntimeException(e);
		}
	}

	public byte[] toBytes() {
		return keyParameter.getKey();
	}

	public KeyParameter getAesKey() {
		return keyParameter;
	}
}
