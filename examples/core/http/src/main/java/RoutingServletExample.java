import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;
import org.slf4j.bridge.SLF4JBridgeHandler;

import static io.datakernel.http.HttpMethod.GET;

public final class RoutingServletExample extends HttpServerLauncher {
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}

	@Provides
	AsyncServlet servlet() {
		return RoutingServlet.create()
				.with(GET, "/", request -> Promise.of(
						HttpResponse.ok200()
								.withHtml("<h1>Go to some pages</h1>" +
										"<a href=\"/path1\"> Path 1 </a><br>" +
										"<a href=\"/path2\"> Path 2 </a><br>" +
										"<a href=\"/path3\"> Path 3 </a>")))
				.with(GET, "/path1", request -> Promise.of(
						HttpResponse.ok200()
								.withHtml("<h1>Hello form the first path!</h1>" +
										"<a href=\"/\">Go home</a>")))
				.with(GET, "/path2", request -> Promise.of(
						HttpResponse.ok200()
								.withHtml("<h1>Hello from the second path!</h1>" +
										"<a href=\"/\">Go home</a>")))
				.with("/*", request -> Promise.of(
						HttpResponse.ofCode(404)
								.withHtml("<h1>404</h1><p>Path '" + request.getRelativePath() + "' not found</p>" +
										"<a href=\"/\">Go home</a>")));
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new RoutingServletExample();
		launcher.launch(args);
	}
}
