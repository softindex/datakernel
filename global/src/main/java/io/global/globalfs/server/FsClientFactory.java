package io.global.globalfs.server;

import io.global.globalfs.api.GlobalFsName;
import io.datakernel.remotefs.FsClient;

public interface FsClientFactory {
	FsClient create(GlobalFsName name);
}
