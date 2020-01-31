package io.global.common.discovery;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.promise.Promise;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import org.jetbrains.annotations.Nullable;
import org.spongycastle.crypto.CryptoException;

import java.util.List;
import java.util.Objects;

import static io.global.common.BinaryDataFormats.REGISTRY;
import static java.util.stream.Collectors.toList;

public final class DiscoveryServiceDriver {
	private static final StructuredCodec<AnnounceData> ANNOUNCE_DATA_CODEC = REGISTRY.get(AnnounceData.class);
	private static final StructuredCodec<SharedSimKey> SHARED_SIM_KEY_CODEC = REGISTRY.get(SharedSimKey.class);

	private final DiscoveryService discoveryService;

	private DiscoveryServiceDriver(DiscoveryService discoveryService) {
		this.discoveryService = discoveryService;
	}

	public static DiscoveryServiceDriver create(DiscoveryService discoveryService) {
		return new DiscoveryServiceDriver(discoveryService);
	}

	public Promise<Void> announce(KeyPair keys, AnnounceData announceData) {
		return discoveryService.announce(keys.getPubKey(), SignedData.sign(ANNOUNCE_DATA_CODEC, announceData, keys.getPrivKey()));
	}

	public Promise<@Nullable AnnounceData> find(PubKey space) {
		return discoveryService.find(space)
				.map(x -> x != null ? x.getValue() : null);
	}

	public Promise<Void> shareKey(PrivKey sender, PubKey receiver, SimKey key) {
		SharedSimKey sharedSimKey = SharedSimKey.of(key, receiver);
		SignedData<SharedSimKey> signed = SignedData.sign(SHARED_SIM_KEY_CODEC, sharedSimKey, sender);
		return discoveryService.shareKey(receiver, signed);
	}

	public Promise<@Nullable SimKey> getSharedKey(KeyPair keys, Hash hash) {
		return discoveryService.getSharedKey(keys.getPubKey(), hash)
				.map(signedSharedSimKey -> {
					if (signedSharedSimKey == null) {
						return null;
					}
					try {
						return signedSharedSimKey.getValue().decryptSimKey(keys.getPrivKey());
					} catch (CryptoException e) {
						throw new UncheckedException(e);
					}
				});
	}

	public Promise<List<SimKey>> getSharedKeys(KeyPair keys) {
		return discoveryService.getSharedKeys(keys.getPubKey())
				.map(list -> list.stream()
						.map(sssk -> {
							try {
								return sssk.getValue().decryptSimKey(keys.getPrivKey());
							} catch (CryptoException e) {
								return null; // just skip invalid shares for now
							}
						})
						.filter(Objects::nonNull)
						.collect(toList()));
	}
}
