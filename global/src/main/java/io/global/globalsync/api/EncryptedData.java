package io.global.globalsync.api;

public final class EncryptedData {
	public final byte[] initializationVector;
	public final byte[] encryptedBytes;

	public EncryptedData(byte[] initializationVector, byte[] encryptedBytes) {
		this.initializationVector = initializationVector;
		this.encryptedBytes = encryptedBytes;
	}
}
