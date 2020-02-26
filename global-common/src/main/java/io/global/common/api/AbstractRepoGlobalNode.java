package io.global.common.api;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractRepoGlobalNode<
		Self extends AbstractRepoGlobalNode<Self, Namespace, Node, RepoStorage>,
		Namespace extends AbstractRepoGlobalNamespace<Namespace, Self, Node, RepoStorage>,
		Node,
		RepoStorage> extends AbstractGlobalNode<Self, Namespace, Node> {

	public AbstractRepoGlobalNode(RawServerId id, DiscoveryService discoveryService, Function<RawServerId, Node> nodeFactory) {
		super(id, discoveryService, nodeFactory);
	}

	public abstract RepoStorageFactory<RepoStorage> getStorageFactory();

	protected <T> Promise<T> simpleRepoMethod(PubKey space, String repo,
			Function<AbstractRepoGlobalNamespace<Namespace, Self, Node, RepoStorage>.Repo, Promise<@Nullable T>> selfResult,
			BiFunction<Node, AbstractRepoGlobalNamespace<Namespace, Self, Node, RepoStorage>.Repo, Promise<T>> remoteMasterResult,
			Supplier<Promise<T>> defaultResult) {
		Namespace ns = ensureNamespace(space);
		return ns.ensureRepository(repo)
				.then(r -> selfResult.apply(r)
						.then(result -> result != null ?
								Promise.of(result) :
								ns.ensureMasterNodes()
										.then(nodes -> isMasterFor(space) ?
												defaultResult.get() :
												Promises.firstSuccessful(nodes.stream().map(master -> (AsyncSupplier<T>) () -> remoteMasterResult.apply(master, r)))
														.thenEx((res, e) -> e != null ? defaultResult.get() : Promise.of(res)))));
	}
}
