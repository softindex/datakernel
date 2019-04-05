package io.global.common;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.IAsyncHttpClient;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.HttpDiscoveryService;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemoryAnnouncementStorage;
import io.global.common.stub.InMemorySharedKeyStorage;
import io.global.ot.server.CommitStorage;
import io.global.ot.stub.CommitStorageStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofList;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static io.global.launchers.GlobalConfigConverters.ofRawServerId;

public final class ExampleCommonModule extends AbstractModule {
	private static final Logger logger = LoggerFactory.getLogger(ExampleCommonModule.class);

	private static final PrivKey DEMO_OT_PRIVATE_KEY =
			PrivKey.of(new BigInteger("52a8fbf6c82e3e177a07d5fb822bbef07c1f28cfaeeb320964a4598ea82159b", 16));

	@Provides
	@Singleton
	DiscoveryService provideDiscoveryService(Eventloop eventloop, IAsyncHttpClient httpClient, Config config) {
		InetSocketAddress discoveryAddress = config.get(ofInetSocketAddress(), "discovery.address", null);
		if (discoveryAddress != null) {
			logger.info("Using remote discovery service at " + discoveryAddress);
			return HttpDiscoveryService.create(discoveryAddress, httpClient);
		}
		logger.info("Using local discovery service");

		List<RawServerId> masters = config.get(ofList(ofRawServerId()), "discovery.masters", new ArrayList<>());
		if (masters.isEmpty()) {
			RawServerId otServerId = config.get(ofRawServerId(), "ot.serverId", null);
			if (otServerId != null) masters.add(otServerId);
		}

		InMemoryAnnouncementStorage announcementStorage = new InMemoryAnnouncementStorage();
		InMemorySharedKeyStorage sharedKeyStorage = new InMemorySharedKeyStorage();

		PubKey demoPubKey = DEMO_OT_PRIVATE_KEY.computePubKey();
		AnnounceData announceData = AnnounceData.of(System.currentTimeMillis(), new HashSet<>(masters));
		SignedData<AnnounceData> signedData = SignedData.sign(REGISTRY.get(AnnounceData.class), announceData, DEMO_OT_PRIVATE_KEY);

		announcementStorage.addAnnouncements(map(demoPubKey, signedData));

		return LocalDiscoveryService.create(eventloop, announcementStorage, sharedKeyStorage);
	}

	@Provides
	@Singleton
	CommitStorage provideCommitStorage() {
		return new CommitStorageStub();
	}

}
