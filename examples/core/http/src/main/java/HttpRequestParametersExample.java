import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.concurrent.Executor;

import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class HttpRequestParametersExample extends HttpServerLauncher {
	private static final String RESOURCE_DIR = "static/query";
	private static final String HTML_TEXT_CENTER = "<h1><div style=\"text-align: center;\"> Hello from %s, %s!</div></h1>";
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
							return HttpResponse.ok200()
									.withHtml(String.format(HTML_TEXT_CENTER, "POST", name));
						}))
				.map(GET, "/hello", request -> {
					String name = request.getQueryParameter("name");
					return HttpResponse.ok200()
							.withHtml(String.format(HTML_TEXT_CENTER, "GET", name));
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
