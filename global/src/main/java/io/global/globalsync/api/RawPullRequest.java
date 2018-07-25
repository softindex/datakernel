package io.global.globalsync.api;

import io.global.common.Signable;

public final class RawPullRequest implements Signable {
	public final byte[] bytes;

	public final RepositoryName repository;
	public final RepositoryName forkRepository;

	private RawPullRequest(byte[] bytes,
			RepositoryName repository, RepositoryName forkRepository) {
		this.bytes = bytes;
		this.repository = repository;
		this.forkRepository = forkRepository;
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}
}
