package io.global.common;

public final class SimKeyHash {
	private final byte[] bytes;

	public SimKeyHash(byte[] bytes) {
		this.bytes = bytes;
	}

	public byte[] toBytes() {
		return bytes;
	}
}
