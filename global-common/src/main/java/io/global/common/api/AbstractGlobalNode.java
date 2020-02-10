package io.global.common.api;

import io.datakernel.common.ApplicationSettings;
import io.datakernel.common.Initializable;
import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.common.PubKey;
import io.global.common.RawServerId;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public abstract class AbstractGlobalNode<S extends AbstractGlobalNode<S, L, N>, L extends AbstractGlobalNamespace<L, S, N>, N>
		implements Initializable<S> {
	public static final Duration DEFAULT_LATENCY_MARGIN = ApplicationSettings.getDuration(AbstractGlobalNode.class, "latencyMargin", Duration.ofSeconds(5));

	private final Set<PubKey> managedPublicKeys = new HashSet<>();

	protected final Map<PubKey, L> namespaces = new HashMap<>();
	protected Duration latencyMargin = DEFAULT_LATENCY_MARGIN;

	private final Function<RawServerId, N> nodeFactory;

	protected final RawServerId id;
	protected final DiscoveryService discoveryService;

	protected CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public AbstractGlobalNode(RawServerId id, DiscoveryService discoveryService, Function<RawServerId, N> nodeFactory) {
		this.id = id;
		this.discoveryService = discoveryService;
		this.nodeFactory = nodeFactory;
	}

	protected abstract L createNamespace(PubKey space);

	@SuppressWarnings("unchecked")
	public S withManagedPublicKey(PubKey space) {
		managedPublicKeys.add(space);
		return (S) this;
	}

	@SuppressWarnings("unchecked")
	public S withManagedPublicKeys(Set<PubKey> managedPublicKeys) {
		this.managedPublicKeys.addAll(managedPublicKeys);
		return (S) this;
	}

	@SuppressWarnings("unchecked")
	public S withLatencyMargin(Duration latencyMargin) {
		this.latencyMargin = latencyMargin;
		return (S) this;
	}

	@SuppressWarnings("unchecked")
	public S withCurrentTimeProvider(CurrentTimeProvider currentTimeProvider) {
		this.now = currentTimeProvider;
		return (S) this;
	}

	public Set<PubKey> getManagedPublicKeys() {
		return managedPublicKeys;
	}

	public Function<RawServerId, N> getNodeFactory() {
		return nodeFactory;
	}

	public RawServerId getId() {
		return id;
	}

	public Duration getLatencyMargin() {
		return latencyMargin;
	}

	public CurrentTimeProvider getCurrentTimeProvider() {
		return now;
	}

	public DiscoveryService getDiscoveryService() {
		return discoveryService;
	}

	// public for testing
	public L ensureNamespace(PubKey space) {
		return namespaces.computeIfAbsent(space, this::createNamespace);
	}

	public boolean isMasterFor(PubKey space) {
		return managedPublicKeys.contains(space);
	}

	protected <T> Promise<T> simpleMethod(PubKey space, Function<N, Promise<T>> self, Function<L, Promise<T>> local) {
		L ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.then(nodes -> {
					if (isMasterFor(space)) {
						return local.apply(ns);
					}
					return Promises.firstSuccessful(Stream.concat(
							nodes.stream().map(globalFsNode -> () -> self.apply(globalFsNode)),
							Stream.of(() -> local.apply(ns))));
				});
	}
}
