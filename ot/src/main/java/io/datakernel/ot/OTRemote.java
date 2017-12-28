package io.datakernel.ot;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

public interface OTRemote<K, D> {
	CompletionStage<K> createId();

	CompletionStage<Void> push(List<OTCommit<K, D>> commits);

	CompletionStage<Set<K>> getHeads();

	CompletionStage<OTCommit<K, D>> loadCommit(K revisionId);

	CompletionStage<Void> saveMerge(Map<K, List<D>> diffs);

	CompletionStage<Map<K, List<D>>> loadMerge(Set<K> nodes);

	CompletionStage<List<D>> loadSnapshot(K revisionId);

	CompletionStage<Void> saveSnapshot(K revisionId, List<D> diffs);

	CompletionStage<Boolean> isSnapshot(K revisionId);
}
