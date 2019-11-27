package io.global.kv;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNamespace;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.KvStorage;
import io.global.kv.api.RawKvItem;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.datakernel.async.function.AsyncSuppliers.coalesce;
import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static io.datakernel.async.process.AsyncExecutors.retry;
import static io.datakernel.promise.Promises.toList;
import static io.global.util.Utils.tolerantCollectVoid;
import static java.util.stream.Collectors.toSet;

public final class GlobalKvNamespace extends AbstractGlobalNamespace<GlobalKvNamespace, GlobalKvNodeImpl, GlobalKvNode> {
	private final Map<String, Repo> repos = new HashMap<>();
	private final AsyncSupplier<Void> updateRepositories = reuse(this::doUpdateRepositories);

	private long updateRepositoriesTimestamp;

	public GlobalKvNamespace(GlobalKvNodeImpl node, PubKey space) {
		super(node, space);
	}

	public Map<String, Repo> getRepos() {
		return repos;
	}

	public Promise<Repo> ensureRepository(String table) {
		return node.getStorageFactory().create(space, table)
				.map(storage -> repos.computeIfAbsent(table, t -> {
					Repo repo = new Repo(storage, t);
					repo.fetch();
					return repo;
				}));
	}

	public Promise<Void> updateRepositories() {
		return updateRepositories.get();
	}

	@NotNull
	private Promise<Void> doUpdateRepositories() {
		if (updateRepositoriesTimestamp > node.getCurrentTimeProvider().currentTimeMillis() - node.getLatencyMargin().toMillis()) {
			return Promise.complete();
		}
		return ensureMasterNodes()
				.then(masters -> toList(masters.stream()
						.map(master -> master.list(space)
								.thenEx((v, e) -> Promise.of(e == null ? v : Collections.<String>emptySet()))))
						.map(lists -> lists.stream().flatMap(Collection::stream).collect(toSet())))
				.whenResult(repoNames -> repoNames.forEach(this::ensureRepository))
				.whenResult($ -> updateRepositoriesTimestamp = node.getCurrentTimeProvider().currentTimeMillis())
				.toVoid();
	}

	final class Repo {
		private final AsyncSupplier<Void> fetch = reuse(this::doFetch);
		private final AsyncSupplier<Void> push = coalesce(AsyncSupplier.cast(this::doPush).withExecutor(retry(node.retryPolicy)));

		final KvStorage storage;
		private final String table;

		long lastFetchTimestamp = 0;
		long lastPushTimestamp = 0;

		private Repo(KvStorage storage, String table) {
			this.storage = storage;
			this.table = table;
		}

		Promise<ChannelConsumer<SignedData<RawKvItem>>> upload() {
			return storage.upload();
		}

		Promise<Void> push() {
			return push.get();
		}

		Promise<Void> fetch() {
			return fetch.get();
		}

		private Promise<Void> doPush() {
			return forEachMaster(this::push);
		}

		private Promise<Void> doFetch() {
			return forEachMaster(this::fetch);
		}

		private Promise<Void> fetch(GlobalKvNode from) {
			long currentTimestamp = node.getCurrentTimeProvider().currentTimeMillis();
			long fetchFromTimestamp = Math.max(0, lastFetchTimestamp - node.getSyncMargin().toMillis());
			return ChannelSupplier.ofPromise(from.download(space, table, fetchFromTimestamp))
					.streamTo(storage.upload())
					.whenResult($ -> lastFetchTimestamp = currentTimestamp);
		}

		private Promise<Void> push(GlobalKvNode into) {
			long currentTimestamp = node.getCurrentTimeProvider().currentTimeMillis();
			long pushFromTimestamp = Math.max(0, lastPushTimestamp - node.getSyncMargin().toMillis());
			return storage.download(pushFromTimestamp)
					.then(supplier -> {
						if (supplier == null) {
							return Promise.complete();
						}
						return supplier.streamTo(into.upload(space, table));
					})
					.whenResult($ -> lastPushTimestamp = currentTimestamp);
		}

		private Promise<Void> forEachMaster(Function<GlobalKvNode, Promise<Void>> action) {
			return ensureMasterNodes()
					.then(masters -> tolerantCollectVoid(masters, action));
		}

	}
}
