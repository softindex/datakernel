package io.global.globalfs.server;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.AsyncSuppliers;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.globalfs.api.GlobalFsFileSystem;
import io.global.globalfs.api.GlobalFsNamespace;
import io.global.globalfs.api.GlobalFsNode;

import java.util.*;

public class LocalGlobalFsNamespace implements GlobalFsNamespace {
	private final Map<String, GlobalFsFileSystem> fileSystems = new HashMap<>();

	private final GlobalFsLocalNode node;
	private final PubKey pubKey;

	private final Map<RawServerId, GlobalFsNode> knownNodes = new HashMap<>();

	private final AsyncSupplier<List<GlobalFsNode>> discoverNodes;
	private final AsyncSupplier<Void> fetch = AsyncSuppliers.reuse(() ->
			Stages.all(fileSystems.values().stream()
					.map(GlobalFsFileSystem::fetch)
					.map(Stage::toTry)));

	private long nodeDiscoveryTimestamp;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public LocalGlobalFsNamespace(GlobalFsLocalNode node, PubKey pubKey) {
		this.node = node;
		this.pubKey = pubKey;

		knownNodes.put(node.getId(), node);

		discoverNodes = AsyncSuppliers.reuse(() -> node.getDiscoveryService().findServers(pubKey))
				.transform(announceData -> {
					Set<RawServerId> newServerIds = announceData.getData().getServerIds();
					knownNodes.keySet().removeIf(t -> !newServerIds.contains(t));
					newServerIds.forEach(id -> knownNodes.computeIfAbsent(id, node.getClientFactory()::create));
					nodeDiscoveryTimestamp = now.currentTimeMillis();
					return new ArrayList<>(knownNodes.values());
				});
	}

	@Override
	public GlobalFsNode getNode() {
		return node;
	}

	@Override
	public PubKey getKey() {
		return pubKey;
	}

	@Override
	public Stage<Set<String>> getFilesystemNames() {
		return Stage.of(Collections.unmodifiableSet(fileSystems.keySet()));
	}

	@Override
	public Stage<GlobalFsFileSystem> getFileSystem(String fsName) {
		return Stage.of(fileSystems.computeIfAbsent(fsName, $ -> node.getFileSystemFactory().create(this, fsName)));
	}

	private boolean areDiscoveredNodesValid() {
		return nodeDiscoveryTimestamp >= now.currentTimeMillis() - node.getSettings().getLatencyMargin().toMillis();
	}

	@Override
	public Stage<List<GlobalFsNode>> findNodes() {
		if (areDiscoveredNodesValid()) {
			return Stage.of(new ArrayList<>(knownNodes.values()));
		}
		return discoverNodes.get();
	}

	public Stage<Void> fetch() {
		return fetch.get();
	}
}
