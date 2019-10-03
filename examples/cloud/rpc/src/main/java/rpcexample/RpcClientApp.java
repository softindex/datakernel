package rpcexample;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.module.Module;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.service.ServiceGraphModule;

import java.security.SecureRandom;

public class RpcClientApp extends Launcher {
	@Inject
	RpcClient client;

	@Override
	protected Module getModule() {
		return Module.create()
				.install(ServiceGraphModule.create())
				.install(RpcExampleClientModule.create());
	}

	@Override
	protected void run() {
		SecureRandom random = new SecureRandom();
		for (int i = 0; i < 50; i++) {
			client.sendRequest(Integer.toHexString(random.nextInt(100_000)), 750);
		}
	}

	public static void main(String[] args) throws Exception {
		RpcClientApp app = new RpcClientApp();
		app.launch(args);
	}
}
