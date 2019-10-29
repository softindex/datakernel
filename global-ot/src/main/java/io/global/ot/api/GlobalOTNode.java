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

package io.global.ot.api;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.SharedKeyManager;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

public interface GlobalOTNode extends SharedKeyManager {
	Promise<Set<String>> list(PubKey pubKey);

	Promise<Void> save(RepoID repositoryId, Map<CommitId, RawCommit> commits);

	default Promise<Void> save(RepoID repositoryId, Set<CommitEntry> commits) {
		return save(repositoryId, commits.stream().collect(toMap(CommitEntry::getCommitId, CommitEntry::getCommit)));
	}

	Promise<Void> saveHeads(RepoID repositoryId, Set<SignedData<RawCommitHead>> newHeads);

	default Promise<Void> saveAndUpdateHeads(RepoID repositoryId, Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> newHeads) {
		return save(repositoryId, commits)
				.then($ -> saveHeads(repositoryId, newHeads));
	}

	Promise<RawCommit> loadCommit(RepoID repositoryId, CommitId id);

	Promise<Void> saveSnapshot(RepoID repositoryId, SignedData<RawSnapshot> encryptedSnapshot);

	Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId id);

	Promise<Set<CommitId>> listSnapshots(RepoID repositoryId, Set<CommitId> remoteSnapshots);

	default AsyncSupplier<Set<SignedData<RawCommitHead>>> pollHeads(RepoID repositoryId) {
		return () -> getHeads(repositoryId);
	}

	Promise<Set<SignedData<RawCommitHead>>> getHeads(RepoID repositoryId);

	Promise<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest);

	Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repositoryId);

	Promise<ChannelSupplier<CommitEntry>> download(RepoID repositoryId, Set<CommitId> startNodes);

	Promise<ChannelConsumer<CommitEntry>> upload(RepoID repositoryId, Set<SignedData<RawCommitHead>> heads);

}
