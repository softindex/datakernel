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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RepositoryName that = (RepositoryName) o;
		if (!pubKey.equals(that.pubKey)) return false;
		return repositoryName.equals(that.repositoryName);
	}

	@Override
	public int hashCode() {
		int result = pubKey.hashCode();
		result = 31 * result + repositoryName.hashCode();
		return result;
	}
}
