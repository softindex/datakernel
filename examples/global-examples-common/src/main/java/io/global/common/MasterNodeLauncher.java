package io.global.common;

import com.google.inject.Inject;
import com.google.inject.Module;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.global.launchers.GlobalNodesModule;

import java.nio.file.Paths;
import java.util.Collection;

import static com.google.inject.util.Modules.override;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;

public final class MasterNodeLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String DEFAULT_LISTEN_ADDRESSES = "*:9000";
	public static final String DEFAULT_SERVER_ID = "http://127.0.0.1:9000";
	public static final String DEFAULT_FS_STORAGE = Paths.get(System.getProperty("java.io.tmpdir"))
			.resolve("fs_storage").toString();

	@Inject
	AsyncHttpServer server;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(() ->
						Config.create()
								.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
								.with("node.serverId", DEFAULT_SERVER_ID)
								.with("fs.storage", DEFAULT_FS_STORAGE))
						.printEffectiveConfig(),
				override(new GlobalNodesModule())
						.with(new ExampleCommonModule())
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new MasterNodeLauncher().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}

}
