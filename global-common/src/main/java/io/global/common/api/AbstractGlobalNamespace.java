package io.global.common.api;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static java.util.stream.Collectors.toList;

public abstract class AbstractGlobalNamespace<S extends AbstractGlobalNamespace<S, L, N>, L extends AbstractGlobalNode<L, S, N>, N> {
	private static final Logger logger = LoggerFactory.getLogger(AbstractGlobalNamespace.class);

	protected final L node;
	protected final PubKey space;

	protected final AsyncSupplier<List<N>> ensureMasterNodes = reuse(this::doEnsureMasterNodes);
	protected final Map<RawServerId, N> masterNodes = new HashMap<>();
	protected long updateNodesTimestamp;
	protected long announceTimestamp;

	private List<N> ensuredNodes = new ArrayList<>();

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public AbstractGlobalNamespace(L node, PubKey space) {
		this.node = node;
		this.space = space;
	}

	public PubKey getSpace() {
		return space;
	}

	public Promise<List<N>> ensureMasterNodes() {
		return ensureMasterNodes.get();
	}

	private Promise<List<N>> doEnsureMasterNodes() {
		if (updateNodesTimestamp > now.currentTimeMillis() - node.getLatencyMargin().toMillis()) {
			return Promise.of(ensuredNodes);
		}
		return node.getDiscoveryService().find(space)
				.mapEx((announceData, e) -> {
					if (e != null || announceData == null) {
						return ensuredNodes;
					}
					AnnounceData announce = announceData.getValue();
					long timestamp = announce.getTimestamp();
					if (timestamp < announceTimestamp) {
						return ensuredNodes;
					}
					Set<RawServerId> newServerIds = new HashSet<>(announce.getServerIds());
					masterNodes.keySet().removeIf(id -> !newServerIds.contains(id));
					if (newServerIds.remove(node.getId())) { // ensure that we are master for the space if it was announced
						if (node.getManagedPublicKeys().add(space)) {
							logger.trace("became a master for {}: {}", space, node);
						}
					} else {
						if (node.getManagedPublicKeys().remove(space)) {
							logger.trace("stopped being a master for {}: {}", space, node);
						}
					}
					newServerIds.forEach(id -> masterNodes.computeIfAbsent(id, node.getNodeFactory()));
					updateNodesTimestamp = now.currentTimeMillis();
					announceTimestamp = timestamp;

					return ensuredNodes = masterNodes.entrySet().stream()
									.sorted(Comparator.comparingInt(entry -> entry.getKey().getPriority()))
									.map(Map.Entry::getValue)
									.collect(toList());
				});
	}
}
