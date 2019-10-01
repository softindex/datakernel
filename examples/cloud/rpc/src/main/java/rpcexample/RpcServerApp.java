package rpcexample;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.module.Module;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;

public class RpcServerApp extends Launcher {
	@Inject
	RpcServer server;

	@Override
	protected Module getModule() {
		return Module.create()
				.install(ServiceGraphModule.create())
				.install(RpcExampleServerModule.create());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		RpcServerApp app = new RpcServerApp();
		app.launch(args);
	}
}
