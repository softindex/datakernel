package io.global.common;

import io.datakernel.config.Config;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.annotation.Provides;
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

	public static final PrivKey DEMO_PRIVATE_KEY =
			PrivKey.of(new BigInteger("52a8fbf6c82e3e177a07d5fb822bbef07c1f28cfaeeb320964a4598ea82159b", 16));
	public static final SimKey DEMO_SIM_KEY = SimKey.of(
			new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});

	@Provides
	DiscoveryService discoveryService(Eventloop eventloop, IAsyncHttpClient httpClient, Config config) {
		InetSocketAddress discoveryAddress = config.get(ofInetSocketAddress(), "discovery.address", null);
		if (discoveryAddress != null) {
			logger.info("Using remote discovery service at " + discoveryAddress);
			return HttpDiscoveryService.create(discoveryAddress, httpClient);
		}
		logger.info("Using local discovery service");

		List<RawServerId> masters = config.get(ofList(ofRawServerId()), "discovery.masters", new ArrayList<>());
		if (masters.isEmpty()) {
			logger.info("No remote master nodes specified, using local node as a master node");
			masters.add(config.get(ofRawServerId(), "node.serverId"));
		} else {
			logger.info("Using servers " + masters + " as master servers");
		}

		InMemoryAnnouncementStorage announcementStorage = new InMemoryAnnouncementStorage();
		InMemorySharedKeyStorage sharedKeyStorage = new InMemorySharedKeyStorage();

		PubKey demoPubKey = DEMO_PRIVATE_KEY.computePubKey();
		AnnounceData announceData = AnnounceData.of(System.currentTimeMillis(), new HashSet<>(masters));
		SignedData<AnnounceData> signedData = SignedData.sign(REGISTRY.get(AnnounceData.class), announceData, DEMO_PRIVATE_KEY);

		announcementStorage.addAnnouncements(map(demoPubKey, signedData));

		return LocalDiscoveryService.create(eventloop, announcementStorage, sharedKeyStorage);
	}

	@Provides
	CommitStorage commitStorage() {
		return new CommitStorageStub();
	}

}
