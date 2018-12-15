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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.SharedKeyManager;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.*;

public interface GlobalOTNode extends SharedKeyManager {
	Promise<Set<String>> list(PubKey pubKey);

	Promise<Void> save(RepoID repositoryId, Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> heads);

	default Promise<Void> save(RepoID repositoryId, RawCommit rawCommit, SignedData<RawCommitHead> rawHead) {
		return save(repositoryId, singletonMap(rawHead.getValue().commitId, rawCommit), singleton(rawHead));
	}

	Promise<RawCommit> loadCommit(RepoID repositoryId, CommitId id);

	final class HeadsInfo {
		private final Set<CommitId> existing;
		private final Set<CommitId> required;

		public HeadsInfo(Set<CommitId> existing, Set<CommitId> required) {
			this.required = required;
			this.existing = existing;
		}

		public Set<CommitId> getExisting() {
			return existing;
		}

		public Set<CommitId> getRequired() {
			return required;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			HeadsInfo that = (HeadsInfo) o;
			if (!existing.equals(that.existing)) return false;
			if (!required.equals(that.required)) return false;
			return true;
		}

		@Override
		public int hashCode() {
			int result = 0;
			result = 31 * result + existing.hashCode();
			result = 31 * result + required.hashCode();
			return result;
		}
	}

	Promise<HeadsInfo> getHeadsInfo(RepoID repositoryId);

	Promise<Void> saveSnapshot(RepoID repositoryId, SignedData<RawSnapshot> encryptedSnapshot);

	Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId id);

	class Heads {
		public final Set<SignedData<RawCommitHead>> newHeads;
		public final Set<CommitId> excludedHeads;

		public Heads(Set<SignedData<RawCommitHead>> newHeads, Set<CommitId> excludedHeads) {
			this.newHeads = newHeads;
			this.excludedHeads = excludedHeads;
		}

		public Set<SignedData<RawCommitHead>> getNewHeads() {
			return newHeads;
		}

		public Set<CommitId> getExcludedHeads() {
			return excludedHeads;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Heads that = (Heads) o;
			if (!newHeads.equals(that.newHeads)) return false;
			return excludedHeads.equals(that.excludedHeads);
		}

		@Override
		public int hashCode() {
			int result = newHeads.hashCode();
			result = 31 * result + excludedHeads.hashCode();
			return result;
		}
	}

	Promise<Heads> getHeads(RepoID repositoryId, Set<CommitId> remoteHeads);

	default Promise<Set<SignedData<RawCommitHead>>> getHeads(RepoID repositoryId) {
		return getHeads(repositoryId, emptySet())
				.thenApply(headsDelta -> headsDelta.newHeads);
	}

	Promise<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest);

	Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repositoryId);

	final class CommitEntry {
		public final CommitId commitId;
		public final RawCommit commit;
		@Nullable
		public final SignedData<RawCommitHead> head;

		public CommitEntry(CommitId commitId, RawCommit commit, @Nullable SignedData<RawCommitHead> head) {
			this.commitId = commitId;
			this.commit = commit;
			this.head = head;
		}

		public static CommitEntry parse(CommitId commitId, RawCommit commit, @Nullable SignedData<RawCommitHead> head) throws ParseException {
			return new CommitEntry(commitId, commit, head); // TODO
		}

		public CommitId getCommitId() {
			return commitId;
		}

		public RawCommit getCommit() {
			return commit;
		}

		@Nullable
		public SignedData<RawCommitHead> getHead() {
			return head;
		}

		public boolean hasHead() {
			return head != null;
		}
	}

	Promise<ChannelSupplier<CommitEntry>> download(RepoID repositoryId,
			Set<CommitId> required, Set<CommitId> existing);

	Promise<ChannelConsumer<CommitEntry>> upload(RepoID repositoryId);

}
