package io.global.globalfs.server;

import io.global.common.RawServerId;
import io.global.globalfs.api.GlobalFsServer;
import io.global.globalsync.api.RawServer;

public interface GlobalFsServerFactory {
	GlobalFsServer create(RawServerId serverId);
}
