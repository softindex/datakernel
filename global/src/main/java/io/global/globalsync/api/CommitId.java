package io.global.globalsync.api;

import java.util.Arrays;

import static io.global.common.CryptoUtils.sha256;

public final class CommitId {
	private final byte[] bytes;

	private CommitId(byte[] bytes) {
		this.bytes = bytes;
	}

	public static CommitId ofBytes(byte[] bytes) {
		return new CommitId(bytes);
	}

	public static CommitId ofCommitData(byte[] bytes) {
		return new CommitId(sha256(bytes));
	}

	public static CommitId ofCommit(RawCommit rawCommit) {
		return ofCommitData(rawCommit.toBytes());
	}

	public byte[] toBytes() {
		return bytes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		CommitId that = (CommitId) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}
}
