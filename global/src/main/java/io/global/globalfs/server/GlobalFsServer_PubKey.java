package io.global.globalfs.server;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.globalfs.api.GlobalFsName;
import io.global.globalfs.api.GlobalFsServer;

import java.time.Duration;
import java.util.*;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.CollectionUtils.difference;
import static java.lang.System.currentTimeMillis;

final class GlobalFsServer_PubKey {
	public final GlobalFsServerImpl globalFsServer;
	public final PubKey pubKey;
	public final Map<String, GlobalFsServer_FileSystem> fileSystems = new HashMap<>();

	public interface Settings {
		Duration getLatencyMargin();

		Settings getRepositorySettings(GlobalFsName fsname);
	}

	private Settings settings;

	private Map<RawServerId, GlobalFsServer> servers = new HashMap<>();

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public long updateServersTimestamp;

	private final AsyncSupplier<Void> updateServers = reuse(this::doUpdateServers);
	private final AsyncSupplier<Void> fetch = reuse(this::doFetch);

	GlobalFsServer_PubKey(GlobalFsServerImpl globalFsServer, PubKey pubKey) {
		this.globalFsServer = globalFsServer;
		this.pubKey = pubKey;
	}

	public GlobalFsServer_FileSystem ensureFileSystem(GlobalFsName name) {
		return fileSystems.computeIfAbsent(name.getFsName(),
				$ -> new GlobalFsServer_FileSystem(
						globalFsServer.fsClientFactory.create(name),
						globalFsServer.checkpointStorageFactory.create(name),
						name,
						this::ensureServers));
	}

	public boolean isPrefetchedServers() {
		return updateServersTimestamp >= now.currentTimeMillis() - settings.getLatencyMargin().toMillis();
	}

	public Stage<List<GlobalFsServer>> ensureServers() {
		if (isPrefetchedServers()) return Stage.of(getSortedServers());
		return updateServers.get()
				.thenApply($ -> getSortedServers());
	}

	List<GlobalFsServer> getSortedServers() {
		return new ArrayList<>(servers.values());
	}

	Stage<Void> doUpdateServers() {
		return globalFsServer.getDiscoveryService().findServers(pubKey)
				.whenResult(announceData -> {
					Set<RawServerId> newServerIds = announceData.getData().getServerIds();
					difference(servers.keySet(), newServerIds).forEach(servers::remove);
					for (RawServerId newServerId : newServerIds) {
						if (servers.containsKey(newServerId)) continue;
						servers.put(newServerId,
								globalFsServer.getRawServerFactory().create(newServerId));
					}
				})
				.thenRun(() -> updateServersTimestamp = currentTimeMillis())
				.toVoid();
	}

	Stage<Void> doFetch() {
		return Stages.all(fileSystems.values().stream()
				.map(GlobalFsServer_FileSystem::fetch)
				.map(Stage::toTry));
	}

}
