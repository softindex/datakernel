import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.launcher.Launcher;
import io.datakernel.promise.Promise;
import io.datakernel.service.ServiceGraphModule;

import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.di.core.Binding.to;

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
		AsyncServlet servlet(Config config) {
			String message = config.get("message");
			return request -> Promise.of(
					HttpResponse.ok200().withPlainText(message));
		}

		@Provides
		@Export
		AsyncHttpServer server(Eventloop eventloop, AsyncServlet servlet, Config config) {
			return AsyncHttpServer.create(eventloop, servlet)
					.withListenPort(config.get(ofInteger(), "port"));
		}
	}

	@Override
	protected Module getModule() {
		return Module.create()
				.install(ServiceGraphModule.create())
				.install(new ServerModule()
						.rebindImport(Config.class, to(rootConfig -> rootConfig.getChild("config1"), Config.class))
						.rebindExport(AsyncHttpServer.class, Key.of(AsyncHttpServer.class, "server1")))
				.install(new ServerModule()
						.rebindImport(Config.class, to(rootConfig -> rootConfig.getChild("config2"), Config.class))
						.rebindExport(AsyncHttpServer.class, Key.of(AsyncHttpServer.class, "server2")))
				.bind(Eventloop.class).to(Eventloop::create)
				.bind(Config.class).toInstance(
						Config.create()
								.with("config1.port", "8080")
								.with("config1.message", "Hello from Server 1")
								.with("config2.port", "8081")
								.with("config2.message", "Hello from Server 2"));
	}

	@Override
	protected void run() throws InterruptedException {
		System.out.println("http://localhost:" + server1.getListenAddresses().get(0).getPort());
		System.out.println("http://localhost:" + server2.getListenAddresses().get(0).getPort());
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		ModuleRebindExample example = new ModuleRebindExample();
		example.launch(args);
	}
	//[END EXAMPLE]
}
