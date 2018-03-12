package io.datakernel.remotefs.boot;

import io.datakernel.config.Config;
import io.datakernel.remotefs.RemoteFsServer;
import io.datakernel.trigger.TriggerRegistry;
import io.datakernel.util.Initializer;

import static io.datakernel.config.ConfigInitializers.ofAbstractServer;

public class ConfigInitializers {
	private ConfigInitializers() {
	}

	public static Initializer<RemoteFsServer> initializeRemoteFsServer(RemoteFsServer remoteFsServer, Config config) {
		return server -> server
				.initialize(ofAbstractServer(config));
	}

	public static void initializeRemoteFsServerTriggers(RemoteFsServer remoteFsServer, TriggerRegistry triggerRegistry, Config config) {
	}
}
