package io.global.globalfs.api;

import io.global.common.PubKey;

public final class GlobalFsName {
	private final PubKey pubKey;
	private final String fsName;

	public GlobalFsName(PubKey pubKey, String fsName) {
		this.pubKey = pubKey;
		this.fsName = fsName;
	}

	public PubKey getPubKey() {
		return pubKey;
	}

	public String getFsName() {
		return fsName;
	}

}
