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

package io.global.ot.server;

import io.datakernel.async.Promise;
import io.global.common.SignedData;
import io.global.ot.api.*;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface CommitStorage {
	Promise<Map<CommitId, SignedData<RawCommitHead>>> getHeads(RepoID repositoryId);

	Promise<Void> updateHeads(RepoID repositoryId, Set<SignedData<RawCommitHead>> newHeads, Set<CommitId> excludedHeads);

	Promise<Boolean> hasCommit(CommitId commitId);

	Promise<Optional<RawCommit>> loadCommit(CommitId commitId);

	Promise<Set<CommitId>> getChildren(CommitId commitId);

	Promise<Boolean> saveCommit(CommitId commitId, RawCommit rawCommit);

	default Promise<Boolean> saveIncompleteCommit(CommitId commitId, RawCommit rawCommit) {
		return saveCommit(commitId, rawCommit)
				.then(saved -> {
					if (saved) return isIncompleteCommit(commitId);
					return Promise.of(false);
				});
	}

	Promise<Boolean> saveSnapshot(SignedData<RawSnapshot> encryptedSnapshot);

	Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId commitId);

	Promise<Set<CommitId>> listSnapshotIds(RepoID repositoryId);

	Promise<Boolean> savePullRequest(SignedData<RawPullRequest> pullRequest);

	Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repository);

	Promise<Void> markCompleteCommits();

	Promise<Boolean> isCompleteCommit(CommitId commitId);

	default Promise<Boolean> isIncompleteCommit(CommitId commitId) {
		return isCompleteCommit(commitId).map(complete -> !complete);
	};
}
