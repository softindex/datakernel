import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import java.net.InetSocketAddress;

import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.di.module.Modules.combine;
//[START EXAMPLE]
public final class ServerScratch extends Launcher {
	private static final int PORT = 8080;
	@Inject
	private AsyncHttpServer server;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	AsyncServlet servlet() {
		return request -> Promise.of(
				HttpResponse.ok200()
						.withPlainText("Hello from HTTP server"));
	}

	@Provides
	AsyncHttpServer server(Eventloop eventloop, AsyncServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.withListenAddress(config.get(ofInetSocketAddress(), Config.THIS));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(PORT)))
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new ServerScratch();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
