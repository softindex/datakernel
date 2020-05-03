import io.datakernel.di.Injector;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.concurrent.Executor;

import static io.datakernel.http.AsyncServletDecorator.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class ServletDecoratorExample extends HttpServerLauncher {
	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	//[START REGION_1]
	@Provides
	AsyncServlet servlet(Executor executor) {
		return loadBody().serve(
				RoutingServlet.create()
						.map(GET, "/", StaticServlet.ofClassPath(executor, "static/wrapper")
								.withMappingTo("page.html"))
						.map(POST, "/", request -> {
							String text = request.getPostParameter("text");
							if (text == null) {
								return HttpResponse.redirect302("/");
							}
							return HttpResponse.ok200().withPlainText("Message: " + text);
						})
						.map(GET, "/failPage", request -> {
							throw new RuntimeException("fail");
						})
						.then(catchRuntimeExceptions())
						.then(mapException(e -> HttpResponse.ofCode(404).withPlainText("Error: " + e))));
	}
	//[END REGION_1]

	public static void main(String[] args) throws Exception {
		Injector.useSpecializer();

		ServletDecoratorExample launcher = new ServletDecoratorExample();
		launcher.launch(args);
	}
}
