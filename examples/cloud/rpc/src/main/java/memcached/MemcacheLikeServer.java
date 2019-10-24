package memcached;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPoolModule;
import io.datakernel.worker.WorkerPools;

//[START REGION_1]
public class MemcacheLikeServer extends Launcher {
	@Inject
	WorkerPool.Instances<RpcServer> instances;

	@Provides
	WorkerPool workerPool(WorkerPools workerPools) {
		return workerPools.createPool(3);
	}

	@Provides
	Config config() {
		return Config.create()
				.with("memcache.buffers", "4")
				.with("memcache.bufferCapacity", "64mb");
	}

	@Override
	protected Module getModule() {
		return Module.create()
				.install(ServiceGraphModule.create())
				.install(MemcacheMultiServerModule.create())
				.install(WorkerPoolModule.create());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		MemcacheLikeServer server = new MemcacheLikeServer();
		server.launch(args);
	}
}
//[END REGION_1]
