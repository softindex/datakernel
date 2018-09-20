package io.global.globalsync.api;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.global.common.Signable;
import io.global.common.SimKeyHash;

import java.io.IOException;
import java.util.Arrays;

import static io.datakernel.util.Preconditions.checkNotNull;
import static io.global.globalsync.util.SerializationUtils.*;

public final class RawSnapshot implements Signable {
	public final byte[] bytes;

	public final RepositoryName repositoryId;
	public final CommitId commitId;
	public final EncryptedData encryptedDiffs;
	public final SimKeyHash simKeyHash;

	private RawSnapshot(byte[] bytes,
			RepositoryName repositoryId, CommitId commitId, EncryptedData encryptedDiffs, SimKeyHash simKeyHash) {
		this.bytes = bytes;
		this.repositoryId = repositoryId;
		this.commitId = commitId;
		this.encryptedDiffs = encryptedDiffs;
		this.simKeyHash = simKeyHash;
	}

	public static RawSnapshot ofBytes(byte[] bytes) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		RepositoryName repositoryId = readRepositoryId(buf);
		CommitId commitId = readCommitId(buf);
		EncryptedData encryptedData = readEncryptedData(buf);
		SimKeyHash simKeyHash = readSimKeyHash(buf);
		return new RawSnapshot(bytes, repositoryId, commitId, encryptedData, simKeyHash);
	}

	public static RawSnapshot of(RepositoryName repositoryId,
			CommitId commitId,
			EncryptedData encryptedDiffs,
			SimKeyHash simKeyHash) {
		ByteBuf buf = ByteBufPool.allocate(sizeof(repositoryId) + sizeof(commitId) + sizeof(encryptedDiffs) + sizeof(simKeyHash));
		writeRepositoryId(buf, repositoryId);
		writeCommitId(buf, commitId);
		writeEncryptedData(buf, encryptedDiffs);
		writeSimKeyHash(buf, simKeyHash);
		return new RawSnapshot(buf.asArray(),
				repositoryId, commitId, encryptedDiffs, simKeyHash);
	}

	@Override
	public byte[] toBytes() {
		return checkNotNull(bytes);
	}

	public CommitId getCommitId() {
		return checkNotNull(commitId);
	}

	public EncryptedData getEncryptedDiffs() {
		return checkNotNull(encryptedDiffs);
	}

	public SimKeyHash getSimKeyHash() {
		return checkNotNull(simKeyHash);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RawSnapshot that = (RawSnapshot) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}
}
