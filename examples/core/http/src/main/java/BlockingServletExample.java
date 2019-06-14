import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;
import org.slf4j.bridge.SLF4JBridgeHandler;

public final class BlockingServletExample extends HttpServerLauncher {
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}
	@Provides
	AsyncServlet servlet() {
		return RoutingServlet.create()
				.with("/", request -> Promise.of(
						HttpResponse.ok200()
								.withHtml("<a href='hardWork'>Do hard work</a>")))
				.with("/hardWork", AsyncServlet.ofBlocking(request -> {
					Thread.sleep(2000); //Hard work
					return HttpResponse.ok200()
							.withHtml("Hard work is done");
				}));
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new BlockingServletExample();
		launcher.launch(args);
	}
}
