package io.global.globalsync.server;

import io.global.common.RawServerId;
import io.global.globalsync.api.RawServer;

public interface RawServerFactory {
	RawServer create(RawServerId rawServerId);
}
