package io.global.globalfs.server;

import io.datakernel.async.Stage;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.GlobalFsException;

import java.util.HashMap;
import java.util.Map;

public class LocalDiscoveryService implements DiscoveryService {
	private final Map<PubKey, SignedData<AnnounceData>> announced = new HashMap<>();

	@Override
	public Stage<SignedData<AnnounceData>> findServers(PubKey pubKey) {
		return Stage.of(announced.get(pubKey));
	}

	@Override
	public Stage<Void> announce(PubKey pubKey, SignedData<AnnounceData> announceData) {
		if (!announceData.verify(pubKey)) {
			return Stage.ofException(new GlobalFsException("Cannot verify announce data"));
		}
		announced.compute(pubKey, ($, existing) ->
				existing == null || existing.getData().getTimestamp() <= announceData.getData().getTimestamp() ?
						announceData :
						existing);
		return Stage.complete();
	}
}
