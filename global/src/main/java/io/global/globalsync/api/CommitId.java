package io.global.globalsync.api;

public final class CommitId {
	private final byte[] bytes;

	private CommitId(byte[] bytes) {
		this.bytes = bytes;
	}

	public static CommitId ofBytes(byte[] bytes) {
		return new CommitId(bytes);
	}

	public byte[] toBytes() {
		return bytes;
	}

}
