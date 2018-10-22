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

import io.datakernel.async.Stage;
import io.global.common.RepoID;
import io.global.common.SignedData;
import io.global.ot.api.*;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface CommitStorage {
	Stage<Map<CommitId, SignedData<RawCommitHead>>> getHeads(RepoID repositoryId);

	Stage<Void> applyHeads(RepoID repositoryId, Set<SignedData<RawCommitHead>> newHeads, Set<CommitId> excludedHeads);

	Stage<Boolean> hasCommit(CommitId commitId);

	Stage<Optional<RawCommit>> loadCommit(CommitId commitId);

	Stage<Set<CommitId>> getChildren(CommitId commitId);

	Stage<Boolean> saveCommit(CommitId commitId, RawCommit rawCommit);

	Stage<Boolean> saveSnapshot(SignedData<RawSnapshot> encryptedSnapshot);

	Stage<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId commitId);

	Stage<Boolean> savePullRequest(SignedData<RawPullRequest> pullRequest);

	Stage<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repository);

	Stage<Void> markCompleteCommits();

	Stage<Boolean> isCompleteCommit(CommitId commitId);
}
