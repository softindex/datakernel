package io.global.common.api;

import io.datakernel.async.Stage;
import io.global.common.PubKey;
import io.global.common.SignedData;

public interface DiscoveryService {
	Stage<SignedData<AnnounceData>> findServers(PubKey pubKey);

	Stage<Void> announce(SignedData<AnnounceData> announceData);
}
