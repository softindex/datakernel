import io.datakernel.di.annotation.Inject;
import io.datakernel.di.module.Module;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;

import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.launchers.initializers.Initializers.ofAsyncComponents;

// [START EXAMPLE]
public class ServerLauncher extends Launcher {
	@Inject
	private RpcServer server;

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create()
						.initialize(ofAsyncComponents()),
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
