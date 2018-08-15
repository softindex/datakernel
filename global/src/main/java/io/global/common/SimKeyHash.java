package io.global.common;

import java.util.Arrays;

public final class SimKeyHash {
	private final byte[] bytes;

	public SimKeyHash(byte[] bytes) {
		this.bytes = bytes;
	}

	public byte[] toBytes() {
		return bytes;
	}

	public static SimKeyHash ofSimKey(SimKey simKey) {
		return new SimKeyHash(CryptoUtils.sha256(simKey.toBytes()));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SimKeyHash that = (SimKeyHash) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}
}
