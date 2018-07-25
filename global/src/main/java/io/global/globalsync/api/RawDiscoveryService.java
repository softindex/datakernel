package io.global.globalsync.api;

import io.datakernel.async.Stage;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;

public interface RawDiscoveryService {
	Stage<SignedData<AnnounceData>> findServers(PubKey pubKey);

	Stage<Void> announce(SignedData<AnnounceData> announceData);
}
