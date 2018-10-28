package io.datakernel.ot;

import io.datakernel.async.Promise;

import java.util.List;

public interface OTRemoteEx<K, D> extends OTRemote<K, D> {
	Promise<Void> cleanup(K revisionId);

	Promise<Void> backup(OTCommit<K, D> commit, List<D> snapshot);
}
