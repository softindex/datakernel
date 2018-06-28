package io.datakernel.ot;

import io.datakernel.async.Stage;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;

public interface OTCommitFactory<K, D> {
	Stage<OTCommit<K, D>> createCommit(Map<K, ? extends List<? extends D>> parentDiffs, long level);

	default Stage<OTCommit<K, D>> createCommit(K parent, List<? extends D> parentDiff, long level) {
		return createCommit(singletonMap(parent, parentDiff), level);
	}
}
