package io.global.globalsync.server;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.datakernel.time.CurrentTimeProvider;
import io.global.globalsync.api.RawDiscoveryService;
import io.global.globalsync.api.RawServer;
import io.global.globalsync.api.RepositoryName;

import java.time.Duration;
import java.util.*;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.CollectionUtils.difference;
import static java.lang.System.currentTimeMillis;

final class RawServer_PubKey {
	private final RawDiscoveryService discoveryService;
	private final CommitStorage commitStorage;

	public final PubKey pubKey;
	public final Map<String, RawServer_Repository> repositories = new HashMap<>();

	public interface Settings {
		Duration getLatencyMargin();

		RawServer_Repository.Settings getRepositorySettings(RepositoryName repositoryId);
	}

	private final Settings settings;

	private Map<RawServerId, RawServer> servers = new HashMap<>();

	RawServerFactory rawServerFactory;
	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public long updateServersTimestamp;

	private final AsyncSupplier<Void> updateServers = reuse(this::doUpdateServers);
	private final AsyncSupplier<Void> fetch = reuse(this::doFetch);

	RawServer_PubKey(RawDiscoveryService discoveryService, CommitStorage commitStorage,
			PubKey pubKey, Settings settings) {
		this.discoveryService = discoveryService;
		this.commitStorage = commitStorage;
		this.pubKey = pubKey;
		this.settings = settings;
	}

	public RawServer_Repository ensureName(RepositoryName repositoryId) {
		return repositories.computeIfAbsent(repositoryId.getRepositoryName(),
				$ -> new RawServer_Repository(commitStorage, this::ensureServers,
						repositoryId,
						settings.getRepositorySettings(repositoryId)));
	}

	List<RawServer> getSortedServers() {
		return new ArrayList<>(servers.values());
	}

	public boolean isPrefetchedServers() {
		return updateServersTimestamp >= now.currentTimeMillis() - settings.getLatencyMargin().toMillis();
	}

	public Stage<List<RawServer>> ensureServers() {
		if (isPrefetchedServers()) return Stage.of(getSortedServers());
		return updateServers.get()
				.thenApply($ -> getSortedServers());
	}

	Stage<Void> doUpdateServers() {
		return discoveryService.findServers(pubKey)
				.whenResult(announceData -> {
					Set<RawServerId> newServerIds = announceData.getData().getServerIds();
					difference(servers.keySet(), newServerIds).forEach(servers::remove);
					for (RawServerId newServerId : newServerIds) {
						if (servers.containsKey(newServerId)) continue;
						servers.put(newServerId,
								rawServerFactory.create(newServerId));
					}
				})
				.thenRun(() -> updateServersTimestamp = currentTimeMillis())
				.toVoid();
	}

	public Stage<Void> fetch() {
		return fetch.get();
	}

	Stage<Void> doFetch() {
		return Stages.all(repositories.values().stream()
				.map(RawServer_Repository::sync)
				.map(Stage::toTry));
	}

}
