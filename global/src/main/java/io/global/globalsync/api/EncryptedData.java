package io.global.globalsync.api;

import io.global.common.CryptoUtils;
import io.global.common.SimKey;

import java.util.Arrays;

public final class EncryptedData {
	public final byte[] initializationVector;
	public final byte[] encryptedBytes;

	public EncryptedData(byte[] initializationVector, byte[] encryptedBytes) {
		this.initializationVector = initializationVector;
		this.encryptedBytes = encryptedBytes;
	}

	public static EncryptedData encrypt(byte[] plainBytes, SimKey simKey) {
		return CryptoUtils.encryptAES(plainBytes, simKey.getAesKey());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		EncryptedData that = (EncryptedData) o;
		if (!Arrays.equals(initializationVector, that.initializationVector)) return false;
		return Arrays.equals(encryptedBytes, that.encryptedBytes);
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(initializationVector);
		result = 31 * result + Arrays.hashCode(encryptedBytes);
		return result;
	}
}
