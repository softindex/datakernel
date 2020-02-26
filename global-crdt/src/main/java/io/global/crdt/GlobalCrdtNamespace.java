package io.global.crdt;

import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.api.AbstractRepoGlobalNamespace;

import java.util.Collection;

public final class GlobalCrdtNamespace extends AbstractRepoGlobalNamespace<GlobalCrdtNamespace, GlobalCrdtNodeImpl, GlobalCrdtNode, CrdtStorage> {
	public GlobalCrdtNamespace(GlobalCrdtNodeImpl node, PubKey space) {
		super(node, space);
	}

	@Override
	protected Promise<? extends Collection<String>> getRepoList(GlobalCrdtNode master, PubKey space) {
		return master.list(space);
	}

	@Override
	protected Promise<Void> push(Repo repo, GlobalCrdtNode into, long fromTimestamp) {
		return repo.getStorage().download(fromTimestamp)
				.then(supplier -> supplier != null ? supplier.streamTo(into.upload(space, repo.getRepo())) : Promise.complete());
	}

	@Override
	protected Promise<Void> fetch(Repo repo, GlobalCrdtNode from, long fromTimestamp) {
		return from.download(space, repo.getRepo(), fromTimestamp)
				.then(supplier -> supplier.streamTo(repo.getStorage().upload()));
	}
}
