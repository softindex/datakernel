package io.global;

import io.datakernel.async.Promise;
import io.datakernel.exception.StacklessException;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;
import io.global.common.api.DiscoveryService;
import io.global.common.api.SharedKeyStorage;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemoryAnnouncementStorage;
import io.global.common.stub.InMemorySharedKeyStorage;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;

import static io.datakernel.util.CollectionUtils.union;
import static io.global.Utils.ANNOUNCE_DATA_CODEC;
import static java.util.Collections.emptySet;

public final class AppDiscoveryService implements DiscoveryService {
	public static final StacklessException READ_ONLY_DISCOVERY_SERVICE = new StacklessException("Discovery service can only retrieve information");
	private static final PrivKey STUB_PK = PrivKey.of(BigInteger.ONE);

	private final DiscoveryService discoveryService;
	private Set<RawServerId> customMasterServers = emptySet();

	private AppDiscoveryService(DiscoveryService discoveryService) {
		this.discoveryService = discoveryService;
	}

	public static AppDiscoveryService create(DiscoveryService discoveryService) {
		return new AppDiscoveryService(discoveryService);
	}

	public static AppDiscoveryService createStub() {
		AnnouncementStorage announcementStorage = new InMemoryAnnouncementStorage();
		SharedKeyStorage sharedKeyStorage = new InMemorySharedKeyStorage();
		return new AppDiscoveryService(LocalDiscoveryService.create(null, announcementStorage, sharedKeyStorage));
	}

	public AppDiscoveryService withCustomMasterServers(Set<RawServerId> customMasterServers) {
		this.customMasterServers = customMasterServers;
		return this;
	}

	public Set<RawServerId> getCustomMasterServers() {
		return customMasterServers;
	}

	@Override
	public Promise<Void> announce(PubKey space, SignedData<AnnounceData> announceData) {
		return Promise.ofException(READ_ONLY_DISCOVERY_SERVICE);
	}

	@Override
	public Promise<@Nullable SignedData<AnnounceData>> find(PubKey space) {
		return discoveryService.find(space)
				.map(signedData -> {
					AnnounceData actual;
					if (signedData == null || !signedData.verify(space)) {
						actual = new AnnounceData(System.currentTimeMillis(), emptySet());
					} else {
						actual = signedData.getValue();
					}
					AnnounceData custom = new AnnounceData(actual.getTimestamp(), union(actual.getServerIds(), customMasterServers));
					return SignedData.sign(ANNOUNCE_DATA_CODEC, custom, STUB_PK);
				});
	}

	@Override
	public Promise<Void> shareKey(PubKey receiver, SignedData<SharedSimKey> simKey) {
		return Promise.ofException(READ_ONLY_DISCOVERY_SERVICE);
	}

	@Override
	public Promise<@Nullable SignedData<SharedSimKey>> getSharedKey(PubKey receiver, Hash hash) {
		return discoveryService.getSharedKey(receiver, hash);
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> getSharedKeys(PubKey receiver) {
		return discoveryService.getSharedKeys(receiver);
	}
}
