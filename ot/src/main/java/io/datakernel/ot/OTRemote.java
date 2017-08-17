package io.datakernel.ot;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

public interface OTRemote<K, D> {
	CompletionStage<K> createId();

	CompletionStage<Void> push(List<OTCommit<K, D>> commits);

	CompletionStage<Set<K>> getHeads();

	CompletionStage<K> getCheckpoint();

	CompletionStage<OTCommit<K, D>> loadCommit(K revisionId);
}
