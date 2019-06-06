package io.global.common.api;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.PubKey;
import io.global.common.RawServerId;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.datakernel.async.AsyncSuppliers.reuse;

public abstract class AbstractGlobalNamespace<S extends AbstractGlobalNamespace<S, L, N>, L extends AbstractGlobalNode<L, S, N>, N> {
	private static final Logger logger = Logger.getLogger(AbstractGlobalNamespace.class.getName());

	protected final L node;
	protected final PubKey space;

	protected final AsyncSupplier<List<N>> ensureMasterNodes = reuse(this::doEnsureMasterNodes);
	protected final Map<RawServerId, N> masterNodes = new HashMap<>();
	protected long updateNodesTimestamp;
	protected long announceTimestamp;

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
			return Promise.of(getMasterNodes());
		}
		return node.getDiscoveryService().find(space)
				.mapEx((announceData, e) -> {
					if (e == null && announceData != null) {
						AnnounceData announce = announceData.getValue();
						if (announce.getTimestamp() >= announceTimestamp) {
							Set<RawServerId> newServerIds = new HashSet<>(announce.getServerIds());
							masterNodes.keySet().removeIf(id -> !newServerIds.contains(id));
							if (newServerIds.remove(node.getId())) { // ensure that we are master for the space if it was announced
								if (node.getManagedPublicKeys().add(space)) {
									logger.log(Level.FINEST, () -> "became a master for " + space + ": " + node);
								}
							} else {
								if (node.getManagedPublicKeys().remove(space)) {
									logger.log(Level.FINEST, () -> "stopped being a master for " + space + ": " + node);
								}
							}
							newServerIds.forEach(id -> masterNodes.computeIfAbsent(id, node.getNodeFactory()));
							updateNodesTimestamp = now.currentTimeMillis();
							announceTimestamp = announce.getTimestamp();
						}
					}
					return getMasterNodes();
				});
	}

	public List<N> getMasterNodes() {
		return new ArrayList<>(masterNodes.values());
	}
}
