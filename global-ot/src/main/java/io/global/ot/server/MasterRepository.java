package io.global.ot.server;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.AsyncSuppliers;
import io.datakernel.async.Promise;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.api.RawCommitHead;
import io.global.ot.api.RawPullRequest;
import io.global.ot.api.RepoID;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

import static java.util.stream.Collectors.toSet;

public final class MasterRepository {
	@NotNull
	private final RawServerId rawServerId;

	@NotNull
	private final RepoID repoID;

	@NotNull
	private final GlobalOTNode node;

	@NotNull
	private Set<SignedData<RawCommitHead>> heads;

	@NotNull
	private final Set<SignedData<RawPullRequest>> pullRequests;

	private final AsyncSupplier<Set<SignedData<RawCommitHead>>> poll = AsyncSuppliers.reuse(this::doPoll);

	public MasterRepository(@NotNull RawServerId rawServerId, @NotNull RepoID repoID, @NotNull GlobalOTNode node,
							@NotNull Set<SignedData<RawCommitHead>> heads, @NotNull Set<SignedData<RawPullRequest>> pullRequests) {
		this.rawServerId = rawServerId;
		this.repoID = repoID;
		this.node = node;
		this.heads = heads;
		this.pullRequests = pullRequests;
	}

	public Promise<Set<SignedData<RawCommitHead>>> poll() {
		return poll.get();
	}

	private Promise<Set<SignedData<RawCommitHead>>> doPoll() {
		return node.pollHeads(repoID, heads.stream().map(head -> head.getValue().getCommitId()).collect(toSet()))
				.whenResult(result -> heads = result);
	}

	@NotNull
	public RawServerId getRawServerId() {
		return rawServerId;
	}

	@NotNull
	public RepoID getRepoID() {
		return repoID;
	}

	@NotNull
	public GlobalOTNode getNode() {
		return node;
	}

	@NotNull
	public Set<SignedData<RawCommitHead>> getHeads() {
		return heads;
	}

	@NotNull
	public Set<SignedData<RawPullRequest>> getPullRequests() {
		return pullRequests;
	}
}
