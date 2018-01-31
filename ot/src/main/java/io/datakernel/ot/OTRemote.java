package io.datakernel.ot;

import io.datakernel.async.Stage;

import java.util.Collection;
import java.util.List;
import java.util.Set;

// TODO: rename OTRemote -> OTRepository
public interface OTRemote<K, D> {
	Stage<K> createCommitId();

	Stage<Void> push(Collection<OTCommit<K, D>> commits);

	Stage<Set<K>> getHeads();

	Stage<OTCommit<K, D>> loadCommit(K revisionId);

	Stage<List<D>> loadSnapshot(K revisionId);

	Stage<Void> saveSnapshot(K revisionId, List<D> diffs);

	Stage<Void> cleanup(K revisionId);

	Stage<Void> backup(K revisionId, List<D> diffs);
}
