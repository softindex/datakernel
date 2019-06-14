import io.datakernel.di.annotation.Inject;
import io.datakernel.di.module.Module;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;
import org.slf4j.bridge.SLF4JBridgeHandler;

import static io.datakernel.di.module.Modules.combine;

// [START EXAMPLE]
public class ServerLauncher extends Launcher {
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}

	@Inject
	private RpcServer server;

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				new ServerModule());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		ServerLauncher launcher = new ServerLauncher();
		launcher.launch(args);
	}
}
// [END EXAMPLE]
