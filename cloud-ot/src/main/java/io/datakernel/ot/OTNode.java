package io.datakernel.ot;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;

import java.util.List;

import static io.datakernel.codec.StructuredCodecs.*;

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

	static <K, D> StructuredCodec<FetchData<K, D>> getFetchDataCodec(StructuredCodec<K> revisionCodec, StructuredCodec<D> diffCodec) {
		return object(OTNode.FetchData::new,
				"id", OTNode.FetchData::getCommitId, revisionCodec,
				"level", OTNode.FetchData::getLevel, LONG_CODEC,
				"diffs", OTNode.FetchData::getDiffs, ofList(diffCodec));
	}
}
