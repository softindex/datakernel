package io.global.common.api;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static io.datakernel.async.function.AsyncSuppliers.coalesce;
import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static io.datakernel.async.process.AsyncExecutors.retry;
import static io.datakernel.promise.Promises.toList;
import static java.util.stream.Collectors.toSet;

public abstract class AbstractRepoGlobalNamespace<
		Self extends AbstractRepoGlobalNamespace<Self, ParentNode, Node, RepoStorage>,
		ParentNode extends AbstractRepoGlobalNode<ParentNode, Self, Node, RepoStorage>,
		Node,
		RepoStorage> extends AbstractGlobalNamespace<Self, ParentNode, Node> {

	private final Map<String, Repo> repos = new HashMap<>();
	private final AsyncSupplier<Void> updateRepositories = reuse(this::doUpdateRepositories);
	private long updateRepositoriesTimestamp;

	public AbstractRepoGlobalNamespace(ParentNode node, PubKey space) {
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
						.map(master -> getRepoList(master, space)
								.thenEx((v, e) -> Promise.of(e == null ? v : Collections.<String>emptyList()))))
						.map(lists -> lists.stream().flatMap(Collection::stream).collect(toSet())))
				.whenResult(repoNames -> repoNames.forEach(this::ensureRepository))
				.whenResult($ -> updateRepositoriesTimestamp = node.getCurrentTimeProvider().currentTimeMillis())
				.toVoid();
	}

	protected abstract Promise<? extends Collection<String>> getRepoList(Node master, PubKey space);

	protected abstract Promise<Void> push(Repo repo, Node into, long fromTimestamp);

	protected abstract Promise<Void> fetch(Repo repo, Node from, long fromTimestamp);

	public final class Repo {
		private final AsyncSupplier<Void> fetch = reuse(this::doFetch);
		private final AsyncSupplier<Void> push = coalesce(AsyncSupplier.cast(this::doPush).withExecutor(retry(node.getRetryPolicy())));

		private final RepoStorage storage;
		private final String repo;

		long lastFetchTimestamp = 0;
		long lastPushTimestamp = 0;

		private Repo(RepoStorage storage, String repo) {
			this.storage = storage;
			this.repo = repo;
		}

		public RepoStorage getStorage() {
			return storage;
		}

		public String getRepo() {
			return repo;
		}

		public Promise<Void> push() {
			return push.get();
		}

		public Promise<Void> fetch() {
			return fetch.get();
		}

		private Promise<Void> doPush() {
			return forEachMaster(this::push);
		}

		private Promise<Void> doFetch() {
			return forEachMaster(this::fetch);
		}

		private Promise<Void> fetch(Node from) {
			long currentTimestamp = node.getCurrentTimeProvider().currentTimeMillis();
			long fromTimestamp = Math.max(0, lastFetchTimestamp - node.getLatencyMargin().toMillis());
			return AbstractRepoGlobalNamespace.this.fetch(this, from, fromTimestamp)
					.whenResult($ -> lastFetchTimestamp = currentTimestamp);
		}

		private Promise<Void> push(Node into) {
			long currentTimestamp = node.getCurrentTimeProvider().currentTimeMillis();
			long fromTimestamp = Math.max(0, lastPushTimestamp - node.getLatencyMargin().toMillis());
			return AbstractRepoGlobalNamespace.this.push(this, into, fromTimestamp)
					.whenResult($ -> lastPushTimestamp = currentTimestamp);
		}
	}
}
