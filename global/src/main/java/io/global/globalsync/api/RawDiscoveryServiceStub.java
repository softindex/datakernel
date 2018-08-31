package io.global.globalsync.api;

import io.datakernel.async.Stage;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;

import java.util.HashMap;

public class RawDiscoveryServiceStub implements RawDiscoveryService {
	private final HashMap<PubKey, SignedData<AnnounceData>> map = new HashMap<>();

	@Override
	public Stage<SignedData<AnnounceData>> findServers(PubKey pubKey) {
		return Stage.of(map.get(pubKey));
	}

	@Override
	public Stage<Void> announce(SignedData<AnnounceData> announceData) {
		map.compute(announceData.getData().getPubKey(),
				(pubKey, existing) ->
						announceData.getData().getTimestamp() > existing.getData().getTimestamp() ? announceData : existing);
		return Stage.complete();
	}
}
