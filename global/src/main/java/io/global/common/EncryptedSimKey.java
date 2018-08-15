package io.global.common;

import java.util.Arrays;

import static io.global.common.CryptoUtils.encryptECIES;

public final class EncryptedSimKey {
	private final byte[] bytes;

	private EncryptedSimKey(byte[] bytes) {
		this.bytes = bytes;
	}

	public static EncryptedSimKey ofBytes(byte[] bytes) {
		return new EncryptedSimKey(bytes);
	}

	public static EncryptedSimKey ofSimKey(SimKey simKey, PubKey pubKey) {
		return ofBytes(encryptECIES(simKey.toBytes(), pubKey.getEcPublicKey()));
	}

	public byte[] toBytes() {
		return bytes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		EncryptedSimKey that = (EncryptedSimKey) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}
}
