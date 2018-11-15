/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.global.ot.stub;

import io.datakernel.async.Promise;
import io.global.common.SignedData;
import io.global.ot.api.*;
import io.global.ot.server.CommitStorage;

import java.util.*;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class CommitStorageStub implements CommitStorage {
	private final Map<CommitId, RawCommit> commits = new HashMap<>();
	private final Map<RepoID, Map<CommitId, SignedData<RawSnapshot>>> snapshots = new HashMap<>();
	private final Map<RepoID, Map<CommitId, SignedData<RawCommitHead>>> heads = new HashMap<>();
	private final Set<CommitId> pendingCompleteCommits = new HashSet<>();
	private final Map<CommitId, Integer> incompleteParentsCount = new HashMap<>();
	private final Map<CommitId, Set<CommitId>> parentToChildren = new HashMap<>();
	private final Map<RepoID, Set<SignedData<RawPullRequest>>> pullRequests = new HashMap<>();

	@Override
	public Promise<Map<CommitId, SignedData<RawCommitHead>>> getHeads(RepoID repositoryId) {
		return Promise.of(heads.getOrDefault(repositoryId, emptyMap()));
	}

	@Override
	public Promise<Void> applyHeads(RepoID repositoryId, Set<SignedData<RawCommitHead>> newHeads, Set<CommitId> excludedHeads) {
		Map<CommitId, SignedData<RawCommitHead>> map = heads.computeIfAbsent(repositoryId, repositoryId1 -> new HashMap<>());
		newHeads.forEach(head -> map.put(head.getData().commitId, head));
		excludedHeads.forEach(map::remove);
		return Promise.complete();
	}

	@Override
	public Promise<Boolean> hasCommit(CommitId commitId) {
		return Promise.of(commits.containsKey(commitId));
	}

	@Override
	public Promise<Optional<RawCommit>> loadCommit(CommitId commitId) {
		return Promise.of(Optional.ofNullable(commits.get(commitId)));
	}

	@Override
	public Promise<Set<CommitId>> getChildren(CommitId commitId) {
		return Promise.of(parentToChildren.getOrDefault(commitId, emptySet()));
	}

	@Override
	public Promise<Boolean> saveCommit(CommitId commitId, RawCommit rawCommit) {
		RawCommit old = commits.put(commitId, rawCommit);
		if (old != null) return Promise.of(false);
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
		return Promise.of(true);
	}

	@Override
	public Promise<Boolean> saveSnapshot(SignedData<RawSnapshot> encryptedSnapshot) {
		SignedData<RawSnapshot> old = snapshots
				.computeIfAbsent(encryptedSnapshot.getData().repositoryId, $ -> new HashMap<>())
				.put(encryptedSnapshot.getData().commitId, encryptedSnapshot);
		return Promise.of(old == null);
	}

	@Override
	public Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId commitId) {
		return Promise.of(Optional.ofNullable(
				snapshots.getOrDefault(repositoryId, emptyMap()).get(commitId)));
	}

	@Override
	public Promise<Boolean> savePullRequest(SignedData<RawPullRequest> pullRequest) {
		pullRequests.computeIfAbsent(pullRequest.getData().repository, $ -> new HashSet<>()).add(pullRequest);
		return null;
	}

	@Override
	public Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repository) {
		return Promise.of(pullRequests.getOrDefault(repository, emptySet()));
	}

	@Override
	public Promise<Void> markCompleteCommits() {
		while (!pendingCompleteCommits.isEmpty()) {
			pendingCompleteCommits.forEach(this::markCompleteCommit);
		}
		return Promise.complete();
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
	public Promise<Boolean> isCompleteCommit(CommitId commitId) {
		return Promise.of(incompleteParentsCount.get(commitId) == 0);
	}

}
