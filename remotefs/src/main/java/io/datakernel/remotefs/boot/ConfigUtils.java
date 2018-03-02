package io.datakernel.remotefs.boot;

import io.datakernel.config.Config;
import io.datakernel.remotefs.RemoteFsServer;

import static io.datakernel.config.ConfigUtils.initializeAbstractServer;

public class ConfigUtils {
	private ConfigUtils() {
	}

	public static void initializeRemoteFsServer(RemoteFsServer remoteFsServer, Config config) {
		initializeAbstractServer(remoteFsServer, config);
	}
}
