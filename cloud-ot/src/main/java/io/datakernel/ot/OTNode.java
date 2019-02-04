package io.datakernel.ot;

import io.datakernel.async.Promise;

import java.util.Collection;
import java.util.List;

import static io.datakernel.async.Promises.sequence;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparingLong;

public interface OTNode<K, D> {
	Promise<OTCommit<K, D>> createCommit(K parent, List<? extends D> diffs, long level);

	default Promise<Void> pushAll(Collection<? extends OTCommit<K, D>> transactions) {
		return sequence(transactions
				.stream()
				.sorted(comparingLong(OTCommit::getLevel))
				.map(commit ->
						() -> push(commit)));
	}

	default Promise<Void> push(OTCommit<K, D> transaction) {
		return pushAll(singletonList(transaction));
	}

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
