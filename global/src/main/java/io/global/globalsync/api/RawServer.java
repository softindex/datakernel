package io.global.globalsync.api;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stage;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.SimKeyHash;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.*;

public interface RawServer {
	Stage<Set<String>> list(PubKey pubKey);

	Stage<Void> save(RepositoryName repositoryId, Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> heads);

	default Stage<Void> save(RepositoryName repositoryId, RawCommit rawCommit, SignedData<RawCommitHead> rawHead) {
		return save(repositoryId, singletonMap(rawHead.getData().commitId, rawCommit), singleton(rawHead));
	}

	Stage<RawCommit> loadCommit(RepositoryName repositoryId, CommitId id);

	final class CommitEntry {
		public final CommitId commitId;
		public final RawCommit rawCommit;
		public final SignedData<RawCommitHead> rawHead;

		public CommitEntry(CommitId commitId, RawCommit rawCommit, @Nullable SignedData<RawCommitHead> rawHead) {
			this.commitId = commitId;
			this.rawCommit = rawCommit;
			this.rawHead = rawHead;
		}

		public CommitId getCommitId() {
			return commitId;
		}

		public RawCommit getRawCommit() {
			return rawCommit;
		}

		public SignedData<RawCommitHead> getRawHead() {
			return rawHead;
		}

		public boolean hasRawHead() {
			return rawHead != null;
		}
	}

	final class HeadsInfo {
		public final Set<CommitId> bases;
		public final Set<CommitId> heads;

		public HeadsInfo(Set<CommitId> bases, Set<CommitId> heads) {
			this.bases = bases;
			this.heads = heads;
		}

		public Set<CommitId> getBases() {
			return bases;
		}

		public Set<CommitId> getHeads() {
			return heads;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			HeadsInfo that = (HeadsInfo) o;
			if (!bases.equals(that.bases)) return false;
			return heads.equals(that.heads);
		}

		@Override
		public int hashCode() {
			int result = bases.hashCode();
			result = 31 * result + heads.hashCode();
			return result;
		}
	}

	Stage<HeadsInfo> getHeadsInfo(RepositoryName repositoryId);

	Stage<SerialSupplier<CommitEntry>> download(RepositoryName repositoryId,
			Set<CommitId> bases, Set<CommitId> heads);

	default SerialSupplier<CommitEntry> downloadStream(RepositoryName repositoryId,
			Set<CommitId> bases, Set<CommitId> heads) {
		return SerialSupplier.ofStage(download(repositoryId, bases, heads));
	}

	Stage<SerialConsumer<CommitEntry>> upload(RepositoryName repositoryId);

	default SerialConsumer<CommitEntry> uploadStream(RepositoryName repositoryId) {
		return SerialConsumer.ofStage(upload(repositoryId));
	}

	Stage<Void> saveSnapshot(RepositoryName repositoryId, SignedData<RawSnapshot> encryptedSnapshot);

	Stage<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepositoryName repositoryId, CommitId id);

	class HeadsDelta {
		public final Set<SignedData<RawCommitHead>> newHeads;
		public final Set<CommitId> excludedHeads;

		public HeadsDelta(Set<SignedData<RawCommitHead>> newHeads, Set<CommitId> excludedHeads) {
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
			HeadsDelta that = (HeadsDelta) o;
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

	Stage<HeadsDelta> getHeads(RepositoryName repositoryId, Set<CommitId> remoteHeads);

	default Stage<Set<SignedData<RawCommitHead>>> getHeads(RepositoryName repositoryId) {
		return getHeads(repositoryId, emptySet())
				.thenApply(headsDelta -> headsDelta.newHeads);
	}

	Stage<Void> shareKey(SignedData<SharedSimKey> simKey);

	Stage<Optional<SignedData<SharedSimKey>>> getSharedKey(PubKey repositoryOwner, PubKey receiver, SimKeyHash simKeyHash);

	Stage<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest);

	Stage<Set<SignedData<RawPullRequest>>> getPullRequests(RepositoryName repositoryId);
}
