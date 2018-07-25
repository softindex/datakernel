package io.global.globalsync.api;

import io.global.common.PubKey;

public final class RepositoryName {
	private final PubKey pubKey;
	private final String repositoryName;

	public RepositoryName(PubKey pubKey, String repositoryName) {
		this.pubKey = pubKey;
		this.repositoryName = repositoryName;
	}

	public PubKey getPubKey() {
		return pubKey;
	}

	public String getRepositoryName() {
		return repositoryName;
	}

}
