package io.datakernel.ot;

import io.datakernel.async.Promise;

import java.util.List;

public interface OTNode<K, D> {
	Promise<Object> createCommit(K parent, List<? extends D> diffs, long level);

	Promise<K> push(Object commitData);

	final class FetchData<K, D> {
		private final K commitId;
		private final long level;
		private final List<D> diffs;

		public FetchData(K commitId, long level, List<D> diffs) {
			this.commitId = commitId;
			this.level = level;
			this.diffs = diffs;
		}

		public K getCommitId() {
			return commitId;
		}

		public long getLevel() {
			return level;
		}

		public List<D> getDiffs() {
			return diffs;
		}
	}

	Promise<FetchData<K, D>> checkout();

	Promise<FetchData<K, D>> fetch(K currentCommitId);

}
