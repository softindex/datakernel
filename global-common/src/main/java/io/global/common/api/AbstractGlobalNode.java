package io.global.common.api;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.common.ApplicationSettings;
import io.datakernel.common.Initializable;
import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.RetryPolicy;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractGlobalNode<
		Self extends AbstractGlobalNode<Self, Namespace, Node>,
		Namespace extends AbstractGlobalNamespace<Namespace, Self, Node>,
		Node> implements Initializable<Self> {

	public static final Duration DEFAULT_LATENCY_MARGIN = ApplicationSettings.getDuration(AbstractGlobalNode.class, "latencyMargin", Duration.ofSeconds(5));
	public static final RetryPolicy<?> DEFAULT_RETRY_POLICY = RetryPolicy.immediateRetry().withMaxTotalRetryCount(10);

	protected final RawServerId id;
	protected final DiscoveryService discoveryService;
	protected final Map<PubKey, Namespace> namespaces = new HashMap<>();

	private final Set<PubKey> managedPublicKeys = new HashSet<>();
	private final Function<RawServerId, Node> nodeFactory;

	protected Duration latencyMargin = DEFAULT_LATENCY_MARGIN;
	protected RetryPolicy<?> retryPolicy = DEFAULT_RETRY_POLICY;

	protected CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public AbstractGlobalNode(RawServerId id, DiscoveryService discoveryService, Function<RawServerId, Node> nodeFactory) {
		this.id = id;
		this.discoveryService = discoveryService;
		this.nodeFactory = nodeFactory;
	}

	protected abstract Namespace createNamespace(PubKey space);

	@SuppressWarnings("unchecked")
	public Self withManagedPublicKey(PubKey space) {
		managedPublicKeys.add(space);
		return (Self) this;
	}

	@SuppressWarnings("unchecked")
	public Self withManagedPublicKeys(Set<PubKey> managedPublicKeys) {
		this.managedPublicKeys.addAll(managedPublicKeys);
		return (Self) this;
	}

	@SuppressWarnings("unchecked")
	public Self withLatencyMargin(Duration latencyMargin) {
		this.latencyMargin = latencyMargin;
		return (Self) this;
	}

	@SuppressWarnings("unchecked")
	public Self withRetryPolicy(RetryPolicy<?> retryPolicy) {
		this.retryPolicy = retryPolicy;
		return (Self) this;
	}

	@SuppressWarnings("unchecked")
	public Self withCurrentTimeProvider(CurrentTimeProvider currentTimeProvider) {
		this.now = currentTimeProvider;
		return (Self) this;
	}

	public Set<PubKey> getManagedPublicKeys() {
		return managedPublicKeys;
	}

	public Function<RawServerId, Node> getNodeFactory() {
		return nodeFactory;
	}

	public RawServerId getId() {
		return id;
	}

	public Duration getLatencyMargin() {
		return latencyMargin;
	}

	public RetryPolicy<?> getRetryPolicy() {
		return retryPolicy;
	}

	public CurrentTimeProvider getCurrentTimeProvider() {
		return now;
	}

	public DiscoveryService getDiscoveryService() {
		return discoveryService;
	}

	// public for testing
	public Namespace ensureNamespace(PubKey space) {
		return namespaces.computeIfAbsent(space, this::createNamespace);
	}

	public boolean isMasterFor(PubKey space) {
		return managedPublicKeys.contains(space);
	}

	protected <T> Promise<T> simpleMethod(PubKey space, Function<Namespace, Promise<@Nullable T>> selfResult, BiFunction<Node, Namespace, Promise<T>> remoteMasterResult, Supplier<Promise<T>> defaultResult) {
		Namespace ns = ensureNamespace(space);
		return selfResult.apply(ns)
				.then(result -> result != null ?
						Promise.of(result) :
						ns.ensureMasterNodes()
								.then(nodes -> isMasterFor(space) ?
										defaultResult.get() :
										Promises.firstSuccessful(nodes.stream().map(master -> (AsyncSupplier<T>) () -> remoteMasterResult.apply(master, ns)))
												.thenEx((res, e) -> e != null ? defaultResult.get() : Promise.of(res))));
	}
}
