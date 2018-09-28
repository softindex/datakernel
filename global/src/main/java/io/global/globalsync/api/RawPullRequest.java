package io.global.globalsync.api;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.global.common.Signable;

import java.io.IOException;
import java.util.Arrays;

import static io.global.globalsync.util.BinaryDataFormats.*;

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

	public static RawPullRequest ofBytes(byte[] bytes) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);

		RepositoryName repository = readRepositoryId(buf);
		RepositoryName forkRepository = readRepositoryId(buf);

		return new RawPullRequest(bytes,
				repository,
				forkRepository);
	}

	public static RawPullRequest of(RepositoryName repository, RepositoryName forkRepository) {
		ByteBuf buf = ByteBufPool.allocate(sizeof(repository) + sizeof(forkRepository));

		writeRepositoryId(buf, repository);
		writeRepositoryId(buf, forkRepository);

		return new RawPullRequest(buf.asArray(),
				repository, forkRepository);
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RawPullRequest that = (RawPullRequest) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}
}
