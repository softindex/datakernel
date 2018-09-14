package io.global.common;

import io.datakernel.async.Stage;
import io.datakernel.exception.StacklessException;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;

import java.util.HashMap;
import java.util.Map;

import static io.datakernel.util.Preconditions.checkArgument;

public class MockDiscoveryService implements DiscoveryService {
	private final Map<PubKey, SignedData<AnnounceData>> data = new HashMap<>();

	@Override
	public Stage<SignedData<AnnounceData>> findServers(PubKey pubKey) {
		SignedData<AnnounceData> signedData = data.get(pubKey);
		return signedData != null ? Stage.of(signedData) : Stage.ofException(new StacklessException("no servers for this key"));
	}

	@Override
	public Stage<Void> announce(PubKey pubKey, SignedData<AnnounceData> announceData) {
		checkArgument(announceData.verify(pubKey), "Cannot verify announce data");
		data.put(pubKey, announceData);
		return Stage.complete();
	}
}
