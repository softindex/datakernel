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

package io.global.ot.util;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.ot.api.*;

import java.net.ConnectException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class FailingGlobalOTNode implements GlobalOTNode {
	public static final Exception ERROR = new ConnectException("Connection failed");

	@Override
	public Promise<Set<String>> list(PubKey pubKey) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<Void> save(RepoID repositoryId, Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> heads) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<RawCommit> loadCommit(RepoID repositoryId, CommitId id) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<HeadsInfo> getHeadsInfo(RepoID repositoryId) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<Void> saveSnapshot(RepoID repositoryId, SignedData<RawSnapshot> encryptedSnapshot) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId id) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<Set<CommitId>> listSnapshots(RepoID repositoryId, Set<CommitId> remoteSnapshots) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<Heads> getHeads(RepoID repositoryId, Set<CommitId> remoteHeads) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<Void> shareKey(PubKey receiver, SignedData<SharedSimKey> simKey) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<SignedData<SharedSimKey>> getSharedKey(PubKey receiver, Hash simKeyHash) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> getSharedKeys(PubKey receiver) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repositoryId) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<ChannelSupplier<CommitEntry>> download(RepoID repositoryId, Set<CommitId> required, Set<CommitId> existing) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<ChannelConsumer<CommitEntry>> upload(RepoID repositoryId) {
		return Promise.ofException(ERROR);
	}

}
