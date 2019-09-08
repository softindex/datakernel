import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.launcher.Launcher;
import io.datakernel.promise.Promise;
import io.datakernel.service.ServiceGraphModule;

//[START EXAMPLE]
public final class CustomHttpServerExample extends Launcher {
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
	AsyncHttpServer server(Eventloop eventloop, AsyncServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet).withListenPort(PORT);
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new CustomHttpServerExample();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
