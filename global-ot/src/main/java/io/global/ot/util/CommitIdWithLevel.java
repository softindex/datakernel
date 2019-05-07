package io.global.ot.util;

import io.global.ot.api.CommitId;
import org.jetbrains.annotations.NotNull;

public final class CommitIdWithLevel implements Comparable<CommitIdWithLevel> {
	private final long level;
	private final CommitId commitId;

	CommitIdWithLevel(long level, CommitId id) {
		this.level = level;
		this.commitId = id;
	}

	public long getLevel() {
		return level;
	}

	public CommitId getCommitId() {
		return commitId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		CommitIdWithLevel level1 = (CommitIdWithLevel) o;

		if (level != level1.level) return false;
		return commitId.equals(level1.commitId);

	}

	@Override
	public int hashCode() {
		int result = (int) (level ^ (level >>> 32));
		result = 31 * result + commitId.hashCode();
		return result;
	}

	@Override
	public int compareTo(@NotNull CommitIdWithLevel other) {
		return compare(this.level, this.commitId, other.level, other.commitId);
	}

	public static int compare(
			long thisLevel, CommitId thisCommitId,
			long otherLevel, CommitId otherCommitId) {
		int result;
		result = -Long.compare(thisLevel, otherLevel);
		if (result != 0) return result;
		byte[] thisBytes = thisCommitId.toBytes();
		byte[] otherBytes = otherCommitId.toBytes();
		result = Integer.compare(thisBytes.length, otherBytes.length);
		if (result != 0) return result;
		for (int i = 0; i < thisBytes.length; i++) {
			result = Byte.compare(thisBytes[i], otherBytes[i]);
			if (result != 0) return result;
		}
		return 0;
	}

}
