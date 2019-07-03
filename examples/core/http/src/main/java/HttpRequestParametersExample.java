import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.*;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.concurrent.Executor;

import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.*;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class HttpRequestParametersExample extends HttpServerLauncher {
	private static final String RESOURCE_DIR = "static/query";

	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	//[START REGION_1]
	@Provides
	AsyncServlet servlet(Executor executor) {
		return RoutingServlet.create()
				.map(POST, "/hello", loadBody()
						.serve(request -> {
							String name = request.getPostParameters().get("name");
							return Promise.of(
									HttpResponse.ok200()
											.withHtml("<h1><center>Hello from POST, " + name + "!</center></h1>"));
						}))
				.map(GET, "/hello", request -> {
					String name = request.getQueryParameter("name");
					return Promise.of(
							HttpResponse.ok200()
									.withHtml("<h1><center>Hello from GET, " + name + "!</center></h1>"));
				})
				.map("/*", StaticServlet.ofClassPath(executor, RESOURCE_DIR)
						.withIndexHtml());
	}
	//[END REGION_1]

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpRequestParametersExample();
		launcher.launch(args);
	}
}
