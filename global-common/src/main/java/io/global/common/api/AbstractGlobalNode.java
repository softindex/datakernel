package io.global.common.api;

import io.datakernel.util.ApplicationSettings;
import io.global.common.PubKey;
import io.global.common.RawServerId;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public abstract class AbstractGlobalNode<S extends AbstractGlobalNode<S, L, N>, L extends AbstractGlobalNamespace<L, S, N>, N> {
	public static final Duration DEFAULT_LATENCY_MARGIN = ApplicationSettings.getDuration(AbstractGlobalNode.class, "latencyMargin", Duration.ofMinutes(5));

	private final Set<PubKey> managedPublicKeys = new HashSet<>();

	protected final Map<PubKey, L> namespaces = new HashMap<>();
	protected Duration latencyMargin = DEFAULT_LATENCY_MARGIN;

	private final Function<RawServerId, N> nodeFactory;

	protected final RawServerId id;
	protected final DiscoveryService discoveryService;

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

	public DiscoveryService getDiscoveryService() {
		return discoveryService;
	}

	public L ensureNamespace(PubKey space) {
		return namespaces.computeIfAbsent(space, this::createNamespace);
	}

	public boolean isMasterFor(PubKey space) {
		return managedPublicKeys.contains(space);
	}
}
