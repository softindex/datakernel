package io.datakernel.ot;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;

import java.util.List;

import static io.datakernel.codec.StructuredCodecs.*;

public interface OTNode<K, D, C> {

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

		public static <K, D> StructuredCodec<FetchData<K, D>> codec(StructuredCodec<K> revisionCodec, StructuredCodec<D> diffCodec) {
			return object(FetchData::new,
					"id", FetchData::getCommitId, revisionCodec,
					"level", FetchData::getLevel, LONG_CODEC,
					"diffs", FetchData::getDiffs, ofList(diffCodec));
		}
	}

	Promise<FetchData<K, D>> checkout();

	Promise<FetchData<K, D>> fetch(K currentCommitId);

	default Promise<FetchData<K, D>> poll(K currentCommitId) {
		return fetch(currentCommitId);
	}

	Promise<C> createCommit(K parent, List<D> diffs, long parentLevel);

	Promise<FetchData<K, D>> push(C commit);
}
