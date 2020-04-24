package advancedrpc;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.ModuleBuilder;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPoolModule;
import io.datakernel.worker.WorkerPools;

public class AdvancedRpcServerApp extends Launcher {
	@Provides
	WorkerPool workerPool(WorkerPools workerPools) {
		return workerPools.createPool(4);
	}

	@Override
	protected Module getModule() {
		return ModuleBuilder.create()
				.install(ServiceGraphModule.create())
				.install(WorkerPoolModule.create())
				.install(AdvancedRpcServerModule.create())
				.build();
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
