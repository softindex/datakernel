package io.global.kv;

import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.api.AbstractRepoGlobalNamespace;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.KvStorage;

import java.util.Collection;

public final class GlobalKvNamespace extends AbstractRepoGlobalNamespace<GlobalKvNamespace, GlobalKvNodeImpl, GlobalKvNode, KvStorage> {
	public GlobalKvNamespace(GlobalKvNodeImpl node, PubKey space) {
		super(node, space);
	}

	@Override
	protected Promise<? extends Collection<String>> getRepoList(GlobalKvNode master, PubKey space) {
		return master.list(space);
	}

	@Override
	protected Promise<Void> push(Repo repo, GlobalKvNode into, long fromTimestamp) {
		return repo.getStorage().download(fromTimestamp)
				.then(supplier -> supplier != null ? supplier.streamTo(into.upload(space, repo.getRepo())) : Promise.complete());
	}

	@Override
	protected Promise<Void> fetch(Repo repo, GlobalKvNode from, long fromTimestamp) {
		return from.download(space, repo.getRepo(), fromTimestamp)
				.then(supplier -> supplier.streamTo(repo.getStorage().upload()));
	}
}
