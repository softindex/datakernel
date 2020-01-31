package io.global.ot.server;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.promise.Promise;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.api.RawCommitHead;
import io.global.ot.api.RawPullRequest;
import io.global.ot.api.RepoID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public final class MasterRepository {
	@NotNull
	private final RawServerId rawServerId;

	@NotNull
	private final RepoID repoID;

	@NotNull
	private final GlobalOTNode node;

	@Nullable
	private Set<SignedData<RawCommitHead>> heads;

	@Nullable
	private Set<SignedData<RawPullRequest>> pullRequests;

	@Nullable
	private AsyncSupplier<Set<SignedData<RawCommitHead>>> poll;

	public MasterRepository(@NotNull RawServerId rawServerId, @NotNull RepoID repoID, @NotNull GlobalOTNode node) {
		this.rawServerId = rawServerId;
		this.repoID = repoID;
		this.node = node;
	}

	public Promise<Set<SignedData<RawCommitHead>>> poll() {
		if (poll == null) {
			poll = node.pollHeads(repoID)
					.map(heads -> this.heads = heads);
		}
		return poll.get();
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

	@Nullable
	public Set<SignedData<RawCommitHead>> getHeads() {
		return heads;
	}

	@Nullable
	public Set<SignedData<RawPullRequest>> getPullRequests() {
		return pullRequests;
	}
}
