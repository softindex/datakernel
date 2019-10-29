package io.global;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.promise.Promise;
import io.global.common.RawServerId;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.HttpDiscoveryService;
import io.global.kv.api.StorageFactory;
import io.global.kv.stub.RuntimeKvStorageStub;
import io.global.ot.server.CommitStorage;
import io.global.ot.stub.CommitStorageStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofList;
import static io.global.launchers.GlobalConfigConverters.ofRawServerId;
import static java.util.Collections.singletonList;

public final class LocalNodeCommonModule extends AbstractModule {
	private static final Logger logger = LoggerFactory.getLogger(LocalNodeCommonModule.class);

	private final RawServerId localServerId;

	public LocalNodeCommonModule(String localServerId) {
		this.localServerId = new RawServerId(localServerId);
	}

	@Provides
	DiscoveryService provideDiscoveryService(Eventloop eventloop, Config config, IAsyncHttpClient client) {
		List<RawServerId> customMastersList = config.get(ofList(ofRawServerId()), "discovery.customMasters", singletonList(localServerId));
		Set<RawServerId> customMasters = new HashSet<>(customMastersList);
		InetSocketAddress discoveryAddress = config.get(ofInetSocketAddress(), "discovery.address", null);
		if (discoveryAddress != null) {
			logger.info("Using remote discovery service at " + discoveryAddress);
			HttpDiscoveryService discoveryService = HttpDiscoveryService.create(discoveryAddress, client);
			return AppDiscoveryService.create(discoveryService)
					.withCustomMasterServers(customMasters);
		} else {
			logger.warn("No discovery.address config found, using discovery stub");
			return AppDiscoveryService.createStub()
					.withCustomMasterServers(customMasters);
		}
	}

	@Provides
	CommitStorage provideCommitStorage() {
		return new CommitStorageStub();
	}

	@Provides
	StorageFactory kvStorageFactory() {
		return (pubKey, table) -> Promise.of(new RuntimeKvStorageStub());
	}

}
