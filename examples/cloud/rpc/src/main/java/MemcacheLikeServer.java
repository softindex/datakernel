import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.memcache.server.MemcacheServerModule;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;

import java.util.concurrent.CompletionStage;

import static io.datakernel.di.module.Modules.combine;

/**
 * @author is Alex Syrotenko (@pantokrator)
 * Created on 06.08.19.
 */
public class MemcacheLikeServer extends Launcher {

	@Provides
	Eventloop eventloop() { return Eventloop.create(); }

	@Provides
	Config config() {
		return Config.create()
				.with("memcache.buffers", "64")
				.with("memcache.bufferCapacity", "128")
				.with("server.listenAddresses", "localhost:8080")
				.overrideWith(Config.ofProperties(System.getProperties()));
	}

	@Inject
	RpcServer memcacheServer;

	@Inject
	Eventloop eventloop;

	@Override
	protected Module getModule() {
		return combine(ServiceGraphModule.create(),
				ConfigModule.create()
				.printEffectiveConfig()
				.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				MemcacheServerModule.create());
	}

	@Override
	protected void run() throws Exception {
		eventloop.run();
		memcacheServer.listen();
	}

	public static void main(String[] args) throws Exception {
		MemcacheLikeServer server = new MemcacheLikeServer();
		server.launch(args);
	}
}
