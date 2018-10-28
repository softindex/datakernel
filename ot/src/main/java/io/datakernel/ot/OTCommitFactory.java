package io.datakernel.ot;

import io.datakernel.async.Promise;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;

public interface OTCommitFactory<K, D> {
	Promise<OTCommit<K, D>> createCommit(Map<K, ? extends List<? extends D>> parentDiffs, long level);

	default Promise<OTCommit<K, D>> createCommit(K parent, List<? extends D> parentDiff, long level) {
		return createCommit(singletonMap(parent, parentDiff), level);
	}
}
