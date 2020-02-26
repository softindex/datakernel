package io.global.common.api;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static io.global.util.Utils.tolerantCollectVoid;
import static java.util.stream.Collectors.toList;

public abstract class AbstractGlobalNamespace<
		Self extends AbstractGlobalNamespace<Self, ParentNode, Node>,
		ParentNode extends AbstractGlobalNode<ParentNode, Self, Node>,
		Node> {

	private static final Logger logger = LoggerFactory.getLogger(AbstractGlobalNamespace.class);

	protected final ParentNode node;
	protected final PubKey space;

	protected final AsyncSupplier<List<Node>> ensureMasterNodes = reuse(this::doEnsureMasterNodes);
	protected final Map<RawServerId, Node> masterNodes = new HashMap<>();
	protected long updateNodesTimestamp;
	protected long announceTimestamp;

	private List<Node> ensuredNodes = new ArrayList<>();

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public AbstractGlobalNamespace(ParentNode node, PubKey space) {
		this.node = node;
		this.space = space;
	}

	public PubKey getSpace() {
		return space;
	}

	public Promise<List<Node>> ensureMasterNodes() {
		return ensureMasterNodes.get();
	}

	protected final Promise<Void> forEachMaster(Function<Node, Promise<Void>> action) {
		return ensureMasterNodes()
				.then(masters -> tolerantCollectVoid(masters, action));
	}

	private Promise<List<Node>> doEnsureMasterNodes() {
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
