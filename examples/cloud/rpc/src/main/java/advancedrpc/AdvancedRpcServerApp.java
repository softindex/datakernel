package advancedrpc;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPoolModule;
import io.datakernel.worker.WorkerPools;

public class AdvancedRpcServerApp extends Launcher {
	@Inject
	WorkerPool.Instances<RpcServer> instances;

	@Provides
	WorkerPool workerPool(WorkerPools workerPools) {
		return workerPools.createPool(4);
	}

	@Override
	protected Module getModule() {
		return Module.create()
				.install(ServiceGraphModule.create())
				.install(WorkerPoolModule.create())
				.install(AdvancedRpcServerModule.create());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		AdvancedRpcServerApp app = new AdvancedRpcServerApp();
		app.launch(args);
	}
}
