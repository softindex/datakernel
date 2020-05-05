package io.datakernel.ot.repository;

import io.datakernel.ot.OTCommit;
import io.datakernel.promise.Promise;

import java.util.List;

public interface OTRepositoryEx<K, D> extends OTRepository<K, D> {
	Promise<Void> cleanup(K revisionId);

	Promise<Void> backup(OTCommit<K, D> commit, List<D> snapshot);
}
