package io.datakernel.ot;

import io.datakernel.async.Stage;

import java.util.List;

public interface OTRemoteEx<K, D> extends OTRemote<K, D> {
	Stage<Void> cleanup(K revisionId);

	Stage<Void> backup(OTCommit<K, D> commit, List<D> snapshot);
}
