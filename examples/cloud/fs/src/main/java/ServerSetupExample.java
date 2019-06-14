import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.module.Module;
import io.datakernel.exception.UncheckedException;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.remotefs.RemoteFsServerLauncher;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.nio.file.Files;

import static io.datakernel.di.module.Modules.combine;

/**
 * This example demonstrates configuring and launching RemoteFsServer.
 */
public class ServerSetupExample extends RemoteFsServerLauncher {
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}

	@Override
	protected Module getOverrideModule() {
		try {
			return combine(
					ConfigModule.create(
							Config.create()
									.with("remotefs.path", Files.createTempDirectory("server_storage").toString())
									.with("remotefs.listenAddresses", "6732")));
		} catch (IOException e) {
			throw new UncheckedException(e);
		}
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
