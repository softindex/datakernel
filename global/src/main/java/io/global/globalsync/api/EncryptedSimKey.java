package io.global.globalsync.api;

public class EncryptedSimKey {
	public final byte[] bytes;

	public EncryptedSimKey(byte[] bytes) {
		this.bytes = bytes;
	}

	public byte[] toBytes() {
		return bytes;
	}
}
