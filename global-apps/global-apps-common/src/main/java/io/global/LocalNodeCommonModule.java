package io.global;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.IAsyncHttpClient;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.HttpDiscoveryService;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemorySharedKeyStorage;
import io.global.kv.api.StorageFactory;
import io.global.kv.stub.RuntimeKvStorageStub;
import io.global.ot.server.CommitStorage;
import io.global.ot.stub.CommitStorageStub;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.InetSocketAddress;

import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.util.Collections.singleton;

public final class LocalNodeCommonModule extends AbstractModule {
	private static final Logger logger = LoggerFactory.getLogger(LocalNodeCommonModule.class);
	private static final StructuredCodec<AnnounceData> ANNOUNCE_DATA_CODEC = REGISTRY.get(AnnounceData.class);
	private static final PrivKey STUB_PK = PrivKey.of(BigInteger.ONE);

	private final RawServerId localServerId;

	public LocalNodeCommonModule(String localServerId) {
		this.localServerId = new RawServerId(localServerId);
	}

	@Provides
	DiscoveryService provideDiscoveryService(Eventloop eventloop, Config config, IAsyncHttpClient client) {
		InetSocketAddress discoveryAddress = config.get(ofInetSocketAddress(), "discovery.address", null);
		if (discoveryAddress != null) {
			logger.info("Using remote discovery service at " + discoveryAddress);
			return HttpDiscoveryService.create(discoveryAddress, client);
		} else {
			logger.warn("No discovery.address config found, using discovery stub");
			AnnouncementStorage announcementStorage = new AnnouncementStorage() {
				@Override
				public Promise<Void> store(PubKey space, SignedData<AnnounceData> announceData) {
					throw new UnsupportedOperationException();
				}

				@Override
				public Promise<@Nullable SignedData<AnnounceData>> load(PubKey space) {
					AnnounceData announceData = AnnounceData.of(System.currentTimeMillis(), singleton(localServerId));
					return Promise.of(SignedData.sign(ANNOUNCE_DATA_CODEC, announceData, STUB_PK));
				}
			};
			InMemorySharedKeyStorage sharedKeyStorage = new InMemorySharedKeyStorage();
			return LocalDiscoveryService.create(eventloop, announcementStorage, sharedKeyStorage);
		}
	}

	@Provides
	CommitStorage provideCommitStorage() {
		return new CommitStorageStub();
	}

	@Provides
	StorageFactory kvStorageFactory() {
		return (pubKey, s) -> Promise.of(new RuntimeKvStorageStub());
	}

}
