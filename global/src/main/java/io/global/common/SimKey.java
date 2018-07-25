package io.global.common;

import org.spongycastle.crypto.params.KeyParameter;

public final class SimKey {
	private final KeyParameter keyParameter;

	public SimKey(KeyParameter keyParameter) {
		this.keyParameter = keyParameter;
	}

	public byte[] toBytes() {
		return null;
	}

	public KeyParameter getAesKey() {
		return null;
	}
}
