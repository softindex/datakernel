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

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.SharedKeyManager;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface GlobalOTNode extends SharedKeyManager {
	Promise<Set<String>> list(PubKey pubKey);

	Promise<Void> save(RepoID repositoryId, Map<CommitId, RawCommit> commits);

	Promise<Void> saveHeads(RepoID repositoryId, Set<SignedData<RawCommitHead>> newHeads);

	default Promise<Void> saveAndUpdateHeads(RepoID repositoryId, Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> newHeads) {
		return save(repositoryId, commits)
				.then($ -> saveHeads(repositoryId, newHeads));
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
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			HeadsInfo that = (HeadsInfo) o;
			if (!existing.equals(that.existing)) {
				return false;
			}
			if (!required.equals(that.required)) {
				return false;
			}
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

	Promise<Set<CommitId>> listSnapshots(RepoID repositoryId, Set<CommitId> remoteSnapshots);

	default AsyncSupplier<Set<SignedData<RawCommitHead>>> pollHeads(RepoID repositoryId) {
		return () -> getHeads(repositoryId);
	}

	Promise<Set<SignedData<RawCommitHead>>> getHeads(RepoID repositoryId);

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
