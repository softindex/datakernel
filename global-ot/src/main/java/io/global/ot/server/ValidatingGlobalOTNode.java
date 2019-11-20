package io.global.ot.server;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.ot.api.*;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.global.ot.util.HttpDataFormats.COMMIT_CODEC;
import static io.global.ot.util.RawCommitChannels.validateConsumer;
import static io.global.ot.util.RawCommitChannels.validateSupplier;

public final class ValidatingGlobalOTNode implements GlobalOTNode {
	public static final StacklessException INVALID_SIGNATURE = new StacklessException(ValidatingGlobalOTNode.class, "Invalid signature");
	public static final StacklessException INVALID_REPOSITORY = new StacklessException(ValidatingGlobalOTNode.class, "Invalid repository");
	public static final StacklessException INVALID_HASH = new StacklessException(ValidatingGlobalOTNode.class, "Invalid hash");
	public static final StacklessException INVALID_COMMIT_ID = new StacklessException(ValidatingGlobalOTNode.class, "Invalid commit id");
	public static final StacklessException INVALID_LEVEL = new StacklessException(ValidatingGlobalOTNode.class, "Invalid level");

	private final GlobalOTNode node;

	private ValidatingGlobalOTNode(GlobalOTNode node) {
		this.node = node;
	}

	public static ValidatingGlobalOTNode create(GlobalOTNode node) {
		return new ValidatingGlobalOTNode(node);
	}

	@Override
	public Promise<Set<String>> list(PubKey pubKey) {
		return node.list(pubKey);
	}

	@Override
	public Promise<Void> save(RepoID repositoryId, Map<CommitId, RawCommit> commits) {
		return Promises.all(commits.entrySet().stream()
				.map(entry -> validateCommit(entry.getKey(), entry.getValue())))
				.then($ -> node.save(repositoryId, commits));
	}

	@Override
	public Promise<Void> saveHeads(RepoID repositoryId, Set<SignedData<RawCommitHead>> newHeads) {
		return Promises.all(newHeads.stream().map(head -> validateHead(repositoryId, head)))
				.then($ -> node.saveHeads(repositoryId, newHeads));
	}

	@Override
	public Promise<RawCommit> loadCommit(RepoID repositoryId, CommitId id) {
		return node.loadCommit(repositoryId, id)
				.then(rawCommit -> validateCommit(id, rawCommit));
	}

	@Override
	public Promise<Void> saveSnapshot(RepoID repositoryId, SignedData<RawSnapshot> encryptedSnapshot) {
		if (!repositoryId.equals(encryptedSnapshot.getValue().getRepositoryId())) {
			return Promise.ofException(INVALID_REPOSITORY);
		}
		if (!encryptedSnapshot.verify(repositoryId.getOwner())) {
			return Promise.ofException(INVALID_SIGNATURE);
		}
		return node.saveSnapshot(repositoryId, encryptedSnapshot);
	}

	@Override
	public Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId id) {
		return node.loadSnapshot(repositoryId, id)
				.then(optional -> {
					if (optional.isPresent()) {
						SignedData<RawSnapshot> snapshot = optional.get();
						if (!snapshot.getValue().getRepositoryId().equals(repositoryId)) {
							return Promise.ofException(INVALID_REPOSITORY);
						}
						if (!snapshot.verify(repositoryId.getOwner())) {
							return Promise.ofException(INVALID_SIGNATURE);
						}
					}
					return Promise.of(optional);
				});
	}

	@Override
	public Promise<Set<CommitId>> listSnapshots(RepoID repositoryId) {
		return node.listSnapshots(repositoryId);
	}

	@Override
	public Promise<Set<SignedData<RawCommitHead>>> getHeads(RepoID repositoryId) {
		return node.getHeads(repositoryId)
				.then(heads -> Promises.all(heads.stream()
						.map(head -> validateHead(repositoryId, head)))
						.map($ -> heads));
	}

	@Override
	public AsyncSupplier<Set<SignedData<RawCommitHead>>> pollHeads(RepoID repositoryId) {
		return node.pollHeads(repositoryId)
				.mapAsync(heads -> Promises.all(heads.stream().map(head -> validateHead(repositoryId, head)))
						.map($ -> heads));
	}

	@Override
	public Promise<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest) {
		RepoID originalRepository = pullRequest.getValue().getRepository();
		if (!pullRequest.verify(originalRepository.getOwner())) {
			return Promise.ofException(INVALID_SIGNATURE);
		}
		return node.sendPullRequest(pullRequest);
	}

	@Override
	public Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repositoryId) {
		return node.getPullRequests(repositoryId)
				.then(pullRequests -> Promises.all(pullRequests.stream()
						.map(pullRequest -> validatePullRequest(repositoryId, pullRequest)))
						.map($ -> pullRequests));
	}

	@Override
	public Promise<ChannelSupplier<CommitEntry>> download(RepoID repositoryId, Set<CommitId> startNodes) {
		return node.download(repositoryId, startNodes)
				.map(supplier -> validateSupplier(supplier, startNodes));
	}

	@Override
	public Promise<ChannelConsumer<CommitEntry>> upload(RepoID repositoryId, Set<SignedData<RawCommitHead>> heads) {
		return Promises.all(heads.stream().map(head -> validateHead(repositoryId, head)))
				.then($ -> node.upload(repositoryId, heads))
				.map(consumer -> validateConsumer(consumer, heads));
	}

	@Override
	public Promise<Void> shareKey(PubKey receiver, SignedData<SharedSimKey> simKey) {
		return node.shareKey(receiver, simKey);
	}

	@Override
	public Promise<@Nullable SignedData<SharedSimKey>> getSharedKey(PubKey receiver, Hash hash) {
		return node.getSharedKey(receiver, hash)
				.then(simKey -> {
					if (simKey != null && !simKey.getValue().getHash().equals(hash)) {
						return Promise.ofException(INVALID_HASH);
					}
					return Promise.of(simKey);
				});
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> getSharedKeys(PubKey receiver) {
		return node.getSharedKeys(receiver);
	}

	// region helpers
	private static Promise<RawCommit> validateCommit(CommitId commitId, RawCommit commit) {
		CommitId expectedCommitId = CommitId.ofCommitData(commit.getLevel(), encodeAsArray(COMMIT_CODEC, commit));
		if (!commitId.equals(expectedCommitId)) {
			return Promise.ofException(INVALID_COMMIT_ID);
		}
		if (commitId.getLevel() != commit.getLevel()) {
			return Promise.ofException(INVALID_LEVEL);
		}
		return Promise.of(commit);
	}

	private static Promise<SignedData<RawCommitHead>> validateHead(RepoID repoId, SignedData<RawCommitHead> head) {
		if (!repoId.equals(head.getValue().getRepositoryId())) {
			return Promise.ofException(INVALID_REPOSITORY);
		}
		return validateSignedData(head, repoId.getOwner());
	}

	private static Promise<SignedData<RawPullRequest>> validatePullRequest(RepoID repoId, SignedData<RawPullRequest> pullRequest) {
		if (!repoId.equals(pullRequest.getValue().getRepository())) {
			return Promise.ofException(INVALID_REPOSITORY);
		}
		return validateSignedData(pullRequest, repoId.getOwner());
	}

	private static <T> Promise<SignedData<T>> validateSignedData(SignedData<T> signedData, PubKey owner) {
		if (!signedData.verify(owner)) {
			return Promise.ofException(INVALID_SIGNATURE);
		}
		return Promise.of(signedData);
	}
	// endregion
}
