import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpResponse;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import static io.datakernel.config.ConfigConverters.ofInteger;

//[START EXAMPLE]
public class ModuleRebindExample extends Launcher {
	@Inject
	@Named("server1")
	AsyncHttpServer server1;

	@Inject
	@Named("server2")
	AsyncHttpServer server2;

	static class ServerModule extends AbstractModule {
		@Provides
		AsyncHttpServer server(Eventloop eventloop, Config config) {
			return AsyncHttpServer.create(eventloop,
					request -> Promise.of(
							HttpResponse.ok200().withPlainText(config.get("message"))))
					.withListenPort(config.get(ofInteger(), "port"));
		}
	}

	@Override
	protected Module getModule() {
		return Module.create()
				.install(ServiceGraphModule.create())
				.install(new ServerModule()
						.rebindImport(Config.class, Binding.to(cfg -> cfg.getChild("modules.1"), Config.class))
						.rebindExport(AsyncHttpServer.class, Key.of(AsyncHttpServer.class, "server1")))
				.install(new ServerModule()
						.rebindImport(Config.class, Binding.to(cfg -> cfg.getChild("modules.2"), Config.class))
						.rebindExport(AsyncHttpServer.class, Key.of(AsyncHttpServer.class, "server2")))
				.bind(Eventloop.class).to(Eventloop::create)
				.bind(Config.class).toInstance(
						Config.create()
								.with("modules.1.port", "80")
								.with("modules.1.message", "Hello from Server 1")
								.with("modules.2.port", "8080")
								.with("modules.2.message", "Hello from Server 2"));
	}

	@Override
	protected void run() throws InterruptedException {
		System.out.println("Server1: http://127.0.0.1:80/ \nServer2: http://127.0.0.1:8080/");
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		ModuleRebindExample example = new ModuleRebindExample();
		example.launch(args);
	}
	//[END EXAMPLE]
}
