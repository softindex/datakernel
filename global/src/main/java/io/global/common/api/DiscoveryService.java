package io.global.common.api;

import io.datakernel.async.Stage;
import io.global.common.KeyPair;
import io.global.common.PubKey;
import io.global.common.SignedData;

public interface DiscoveryService {
	Stage<SignedData<AnnounceData>> findServers(PubKey pubKey);

	Stage<Void> announce(PubKey pubKey, SignedData<AnnounceData> announceData);

	default Stage<Void> announce(KeyPair keys, AnnounceData announceData) {
		return announce(keys.getPubKey(), SignedData.sign(announceData, keys.getPrivKey()));
	}
}
