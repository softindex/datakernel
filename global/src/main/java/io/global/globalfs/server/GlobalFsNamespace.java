package io.global.globalfs.server;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.AsyncSuppliers;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.globalfs.api.GlobalFsClient;
import io.global.globalfs.api.GlobalFsFileSystem;
import io.global.globalfs.api.GlobalFsName;

import java.util.*;

public class GlobalFsNamespace {
	private final Map<String, GlobalFsFileSystem> fileSystems = new HashMap<>();

	private final GlobalFsClientLocalImpl server;
	private final PubKey pubKey;

	private final Map<RawServerId, GlobalFsClient> knownServers = new HashMap<>();

	private final AsyncSupplier<List<GlobalFsClient>> discoverServers;
	private final AsyncSupplier<Void> fetch = AsyncSuppliers.reuse(() ->
			Stages.all(fileSystems.values().stream()
					.map(GlobalFsFileSystem::fetch)
					.map(Stage::toTry)));

	private long serverDiscoveryTimestamp;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public GlobalFsNamespace(GlobalFsClientLocalImpl server, PubKey pubKey) {
		this.server = server;
		this.pubKey = pubKey;

		knownServers.put(server.getId(), server);

		discoverServers = AsyncSuppliers.reuse(() -> server.getDiscoveryService().findServers(pubKey))
				.transform(announceData -> {
					Set<RawServerId> newServerIds = announceData.getData().getServerIds();
					knownServers.keySet().removeIf(t -> !newServerIds.contains(t));
					newServerIds.forEach(id -> knownServers.computeIfAbsent(id, server.getClientFactory()::create));
					serverDiscoveryTimestamp = now.currentTimeMillis();
					return new ArrayList<>(knownServers.values());
				});
	}

	// region getters
	public GlobalFsClientLocalImpl getServer() {
		return server;
	}

	public PubKey getPubKey() {
		return pubKey;
	}
	// endregion

	public Set<String> getFsNames() {
		return Collections.unmodifiableSet(fileSystems.keySet());
	}

	public GlobalFsFileSystem getFileSystem(String fsName) {
		GlobalFsName name = new GlobalFsName(pubKey, fsName);
		return fileSystems.computeIfAbsent(fsName, $ -> server.getFileSystemFactory().create(this, name));
	}

	private boolean areDiscoveredServersValid() {
		return serverDiscoveryTimestamp >= now.currentTimeMillis() - server.getSettings().getLatencyMargin().toMillis();
	}

	public Stage<List<GlobalFsClient>> getServers() {
		if (areDiscoveredServersValid()) {
			return Stage.of(new ArrayList<>(knownServers.values()));
		}
		return discoverServers.get();
	}

	public Stage<Void> fetch() {
		return fetch.get();
	}
}
