package io.global.common.api;

import io.datakernel.util.ApplicationSettings;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class LocalGlobalNode<S extends LocalGlobalNode<S, L, N>, L extends GlobalNamespace<L, S, N>, N> {
	private static final Logger logger = LoggerFactory.getLogger(LocalGlobalNode.class);

	public static final Duration DEFAULT_LATENCY_MARGIN = ApplicationSettings.getDuration(LocalGlobalNode.class, "latencyMargin", Duration.ofMinutes(5));

	private final Set<PubKey> managedPublicKeys = new HashSet<>();

	protected final Map<PubKey, L> namespaces = new HashMap<>();
	protected Duration latencyMargin = DEFAULT_LATENCY_MARGIN;

	private final Function<RawServerId, N> nodeFactory;

	protected final RawServerId id;
	protected final DiscoveryService discoveryService;
	protected final BiFunction<S, PubKey, L> namespaceFactory;

	public LocalGlobalNode(RawServerId id, DiscoveryService discoveryService, Function<RawServerId, N> nodeFactory, BiFunction<S, PubKey, L> namespaceFactory) {
		this.id = id;
		this.discoveryService = discoveryService;
		this.nodeFactory = nodeFactory;
		this.namespaceFactory = namespaceFactory;
	}

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

	@SuppressWarnings("unchecked")
	public L ensureNamespace(PubKey space) {
		return namespaces.computeIfAbsent(space, k -> namespaceFactory.apply((S) this, k));
	}

	public boolean isMasterFor(PubKey space) {
		return managedPublicKeys.contains(space);
	}
}
