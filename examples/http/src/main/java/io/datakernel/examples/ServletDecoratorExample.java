package io.datakernel.examples;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.Collection;

import static io.datakernel.http.AsyncServletDecorator.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.loader.StaticLoaders.ofClassPath;
import static io.datakernel.util.CollectionUtils.list;
import static java.lang.Boolean.parseBoolean;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class ServletDecoratorExample extends HttpServerLauncher {
	@Override
	protected Collection<Module> getBusinessLogicModules() {
		return list(new AbstractModule() {
			@Provides
			@Singleton
			AsyncServlet provide(Eventloop eventloop) {
				return wrappedDecoratorOf(
						mapException($ -> HttpResponse.ofCode(404).withPlainText("Error page")),
						catchRuntimeExceptions(),
						loadBody())
						.serve(RoutingServlet.create()
								.with(GET, "/", SingleResourceStaticServlet.create(eventloop,
										ofClassPath(newSingleThreadExecutor(), "static/wrapper"), "page.html"))
								.with(POST, "/", request -> {
									String text = request.getPostParameter("text");
									if (text == null) {
										return Promise.of(HttpResponse.redirect302("/"));
									}
									return Promise.of(HttpResponse.ok200().withPlainText(text));
								})
								.with(GET, "/failPage", request -> {
									throw new RuntimeException();
								}));
			}
		});
	}

	public static void main(String[] args) throws Exception {
		ServletDecoratorExample launcher = new ServletDecoratorExample();
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
