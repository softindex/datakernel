package io.global.globalsync.client;

import io.datakernel.async.Stage;
import io.global.globalsync.api.CommitId;
import io.global.globalsync.api.RepositoryName;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRemote;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.datakernel.util.CollectionUtils.union;
import static java.util.Collections.singleton;

public final class OTRemoteAdapter<D> implements OTRemote<CommitId, D> {
	private final OTDriver driver;
	private final MyRepositoryId<D> myRepositoryId;
	private final Set<RepositoryName> originRepositoryIds;

	public OTRemoteAdapter(OTDriver driver,
			MyRepositoryId<D> myRepositoryId, Set<RepositoryName> originRepositoryIds) {
		this.driver = driver;
		this.myRepositoryId = myRepositoryId;
		this.originRepositoryIds = originRepositoryIds;
	}

	@Override
	public Stage<OTCommit<CommitId, D>> createCommit(Map<CommitId, ? extends List<? extends D>> parentDiffs, long level) {
		return Stage.of(driver.createCommit(myRepositoryId, parentDiffs, level));
	}

	@Override
	public Stage<Void> push(OTCommit<CommitId, D> commit) {
		return driver.push(myRepositoryId, commit);
	}

	@Override
	public Stage<Set<CommitId>> getHeads() {
		return driver.getHeads(union(singleton(myRepositoryId.getRepositoryId()), originRepositoryIds));
	}

	@Override
	public Stage<OTCommit<CommitId, D>> loadCommit(CommitId revisionId) {
		return driver.loadCommit(myRepositoryId, originRepositoryIds, revisionId);
	}

	@Override
	public Stage<Optional<List<D>>> loadSnapshot(CommitId revisionId) {
		return driver.loadSnapshot(myRepositoryId, originRepositoryIds, revisionId);
	}

	@Override
	public Stage<Void> saveSnapshot(CommitId revisionId, List<D> diffs) {
		return driver.saveSnapshot(myRepositoryId, revisionId, diffs);
	}
}
