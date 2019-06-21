import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launchers.http.HttpServerLauncher;

import static io.datakernel.http.AsyncServletDecorator.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.loader.StaticLoader.ofClassPath;

public final class ServletDecoratorExample extends HttpServerLauncher {
	//[START REGION_1]
	@Provides
	AsyncServlet servlet() {
		return loadBody().serve(
				RoutingServlet.create()
						.with(GET, "/", StaticServlet.create(ofClassPath("static/wrapper"))
								.withMappingTo("page.html"))
						.with(POST, "/", request -> {
							String text = request.getPostParameter("text");
							if (text == null) {
								return Promise.of(
										HttpResponse.redirect302("/"));
							}
							return Promise.of(
									HttpResponse.ok200().withPlainText("Message: " + text));
						})
						.with(GET, "/failPage", request -> {
							throw new RuntimeException("fail");
						})
						.then(catchRuntimeExceptions())
						.then(mapException(e -> HttpResponse.ofCode(404).withPlainText("Error: " + e))));
	}
	//[END REGION_1]

	public static void main(String[] args) throws Exception {
		ServletDecoratorExample launcher = new ServletDecoratorExample();
		launcher.launch(args);
	}
}
