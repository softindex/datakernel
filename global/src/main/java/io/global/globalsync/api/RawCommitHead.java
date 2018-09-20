package io.global.globalsync.api;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.global.common.Signable;

import java.io.IOException;
import java.util.Arrays;

import static io.global.globalsync.util.SerializationUtils.*;

public final class RawCommitHead implements Signable {
	public final byte[] bytes;

	public final RepositoryName repositoryId;
	public final CommitId commitId;
	public final long timestamp;

	private RawCommitHead(byte[] bytes,
			RepositoryName repositoryId, CommitId commitId, long timestamp) {
		this.bytes = bytes;
		this.repositoryId = repositoryId;
		this.commitId = commitId;
		this.timestamp = timestamp;
	}

	public static RawCommitHead ofBytes(byte[] bytes) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		RepositoryName repositoryId = readRepositoryId(buf);
		CommitId commitId = readCommitId(buf);
		long timestamp = buf.readLong();
		return new RawCommitHead(bytes, repositoryId, commitId, timestamp);
	}

	public static RawCommitHead of(RepositoryName repositoryId, CommitId commitId, long timestamp) {
		ByteBuf buf = ByteBufPool.allocate(sizeof(repositoryId) + sizeof(commitId) + 8);
		writeRepositoryId(buf, repositoryId);
		writeCommitId(buf, commitId);
		buf.writeLong(timestamp);
		return new RawCommitHead(buf.asArray(),
				repositoryId, commitId, timestamp);
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	public RepositoryName getRepositoryId() {
		return repositoryId;
	}

	public CommitId getCommitId() {
		return commitId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RawCommitHead that = (RawCommitHead) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}
}
