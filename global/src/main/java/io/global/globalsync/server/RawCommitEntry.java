package io.global.globalsync.server;

import io.global.globalsync.api.CommitId;
import io.global.globalsync.api.RawCommit;

final class RawCommitEntry implements Comparable<RawCommitEntry> {
	public final CommitId commitId;
	public final RawCommit rawCommit;

	public RawCommitEntry(CommitId commitId, RawCommit rawCommit) {
		this.commitId = commitId;
		this.rawCommit = rawCommit;
	}

	public CommitId getCommitId() {
		return commitId;
	}

	public RawCommit getRawCommit() {
		return rawCommit;
	}

	@Override
	public int compareTo(RawCommitEntry other) {
		return Long.compare(this.rawCommit.getLevel(), other.rawCommit.getLevel());
	}
}
