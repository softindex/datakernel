package io.global.globalfs.api;

import io.datakernel.async.Stage;
import io.global.common.api.AnnounceData;
import io.global.common.PubKey;
import io.global.common.SignedData;

public interface GlobalFsDiscoveryService {
	Stage<SignedData<AnnounceData>> findServers(PubKey pubKey);

	Stage<Void> announce(SignedData<AnnounceData> announceData);
}
