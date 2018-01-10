package io.datakernel.ot;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

// TODO: rename OTRemote -> OTRepository
public interface OTRemote<K, D> {
	CompletionStage<K> createId();

	CompletionStage<Void> push(Collection<OTCommit<K, D>> commits);

	CompletionStage<Set<K>> getHeads();

	CompletionStage<OTCommit<K, D>> loadCommit(K revisionId);

	CompletionStage<List<D>> loadSnapshot(K revisionId);

	CompletionStage<Void> saveSnapshot(K revisionId, List<D> diffs);

	CompletionStage<Boolean> isSnapshot(K revisionId);
}
