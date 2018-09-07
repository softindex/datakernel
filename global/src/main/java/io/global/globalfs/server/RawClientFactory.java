package io.global.globalfs.server;

import io.global.common.RawServerId;
import io.global.globalfs.api.GlobalFsClient;

public interface RawClientFactory {
	GlobalFsClient create(RawServerId serverId);
}
