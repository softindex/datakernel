package io.global.globalfs.server;

import io.global.common.RawServerId;
import io.global.globalfs.api.GlobalFsNode;

public interface RawNodeFactory {
	GlobalFsNode create(RawServerId serverId);
}
