import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.remotefs.RemoteFsServerLauncher;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This example demonstrates configuring and launching RemoteFsServer.
 */
public class ServerSetupExample extends RemoteFsServerLauncher {
	private Path storage;

	@Override
	protected void onInit(Injector injector) throws Exception {
		storage = Files.createTempDirectory("server_storage");
	}

	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Provides
			Config config() {
				return Config.create()
						.with("remotefs.path", storage.toString())
						.with("remotefs.listenAddresses", "6732");
			}
		};
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new ServerSetupExample();
		launcher.launch(args);
	}
}
