package io.global.globalsync.stub;

import io.datakernel.async.Stage;
import io.global.common.SignedData;
import io.global.globalsync.api.*;
import io.global.globalsync.server.CommitStorage;

import java.util.*;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class CommitStorageStub implements CommitStorage {
	private final Map<CommitId, RawCommit> commits = new HashMap<>();
	private final Map<RepositoryName, Map<CommitId, SignedData<RawSnapshot>>> snapshots = new HashMap<>();
	private final Map<RepositoryName, Map<CommitId, SignedData<RawCommitHead>>> heads = new HashMap<>();
	private final Set<CommitId> pendingCompleteCommits = new HashSet<>();
	private final Map<CommitId, Integer> incompleteParentsCount = new HashMap<>();
	private final Map<CommitId, Set<CommitId>> parentToChildren = new HashMap<>();
	private final Map<RepositoryName, Set<SignedData<RawPullRequest>>> pullRequests = new HashMap<>();

	@Override
	public Stage<Map<CommitId, SignedData<RawCommitHead>>> getHeads(RepositoryName repositoryId) {
		return Stage.of(heads.getOrDefault(repositoryId, emptyMap()));
	}

	@Override
	public Stage<Void> updateHeads(RepositoryName repositoryId, Set<SignedData<RawCommitHead>> addHeads, Set<CommitId> excludeHeads) {
		Map<CommitId, SignedData<RawCommitHead>> map = heads.computeIfAbsent(repositoryId, repositoryId1 -> new HashMap<>());
		addHeads.forEach(head -> map.put(head.getData().commitId, head));
		excludeHeads.forEach(map::remove);
		return Stage.of(null);
	}

	@Override
	public Stage<Boolean> hasCommit(CommitId commitId) {
		return Stage.of(commits.containsKey(commitId));
	}

	@Override
	public Stage<Optional<RawCommit>> loadCommit(CommitId commitId) {
		return Stage.of(Optional.ofNullable(commits.get(commitId)));
	}

	@Override
	public Stage<Set<CommitId>> getChildren(CommitId commitId) {
		return Stage.of(parentToChildren.getOrDefault(commitId, emptySet()));
	}

	@Override
	public Stage<Boolean> saveCommit(CommitId commitId, RawCommit rawCommit) {
		RawCommit old = commits.put(commitId, rawCommit);
		if (old != null) return Stage.of(false);
		for (CommitId parentId : rawCommit.getParents()) {
			parentToChildren.computeIfAbsent(parentId, $ -> new HashSet<>()).add(commitId);
		}
		int incompleteParents = (int) rawCommit.getParents().stream()
				.filter(parentId -> incompleteParentsCount.get(parentId) != 0)
				.count();
		incompleteParentsCount.put(commitId, incompleteParents);
		if (incompleteParents == 0) {
			pendingCompleteCommits.add(commitId);
		}
		return Stage.of(true);
	}

	@Override
	public Stage<Boolean> saveSnapshot(SignedData<RawSnapshot> encryptedSnapshot) {
		SignedData<RawSnapshot> old = snapshots
				.computeIfAbsent(encryptedSnapshot.getData().repositoryId, $ -> new HashMap<>())
				.put(encryptedSnapshot.getData().commitId, encryptedSnapshot);
		return Stage.of(old == null);
	}

	@Override
	public Stage<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepositoryName repositoryId, CommitId commitId) {
		return Stage.of(Optional.ofNullable(
				snapshots.getOrDefault(repositoryId, emptyMap()).get(commitId)));
	}

	@Override
	public Stage<Boolean> savePullRequest(SignedData<RawPullRequest> pullRequest) {
		this.pullRequests.computeIfAbsent(pullRequest.getData().repository, $ -> new HashSet<>()).add(pullRequest);
		return null;
	}

	@Override
	public Stage<Set<SignedData<RawPullRequest>>> getPullRequests(RepositoryName repository) {
		return Stage.of(pullRequests.getOrDefault(repository, emptySet()));
	}

	@Override
	public Stage<Void> markCompleteCommits() {
		while (!pendingCompleteCommits.isEmpty()) {
			pendingCompleteCommits.forEach(this::markCompleteCommit);
		}
		return Stage.of(null);
	}

	@SuppressWarnings("ConstantConditions")
	void markCompleteCommit(CommitId completeCommitId) {
		assert pendingCompleteCommits.contains(completeCommitId);
		pendingCompleteCommits.remove(completeCommitId);
		for (CommitId childId : parentToChildren.get(completeCommitId)) {
			if (incompleteParentsCount.computeIfPresent(childId, ($, incompleteCount) -> incompleteCount - 1) == 0) {
				pendingCompleteCommits.add(childId);
			}
		}
	}

	@Override
	public Stage<Boolean> isCompleteCommit(CommitId commitId) {
		return Stage.of(incompleteParentsCount.get(commitId) == 0);
	}

}
