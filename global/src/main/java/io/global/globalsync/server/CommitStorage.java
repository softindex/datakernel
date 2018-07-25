package io.global.globalsync.server;

import io.datakernel.async.Stage;
import io.global.common.SignedData;
import io.global.globalsync.api.*;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface CommitStorage {
	Stage<Map<CommitId, SignedData<RawCommitHead>>> getHeads(RepositoryName repositoryId);

	Stage<Void> updateHeads(RepositoryName repositoryId, Set<SignedData<RawCommitHead>> addHeads, Set<CommitId> excludeHeads);

	default Stage<Void> updateHeads(RepositoryName repositoryId, RawServer.HeadsDelta headsDelta) {
		return updateHeads(repositoryId, headsDelta.newHeads, headsDelta.excludedHeads);
	}

	Stage<Boolean> hasCommit(CommitId commitId);

	Stage<Optional<RawCommit>> loadCommit(CommitId commitId);

	Stage<Set<CommitId>> getChildren(CommitId commitId);

	Stage<Boolean> saveCommit(CommitId commitId, RawCommit rawCommit);

	Stage<Boolean> saveSnapshot(SignedData<RawSnapshot> encryptedSnapshot);

	Stage<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepositoryName repositoryId, CommitId commitId);

	Stage<Boolean> savePullRequest(SignedData<RawPullRequest> pullRequest);

	Stage<Set<SignedData<RawPullRequest>>> getPullRequests(RepositoryName repository);

	Stage<Void> markCompleteCommits();

	Stage<Boolean> isCompleteCommit(CommitId commitId);
}
