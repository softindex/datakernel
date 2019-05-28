package io.datakernel.examples;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.http.session.SessionServlet;
import io.datakernel.http.session.SessionStore;
import io.datakernel.http.session.SessionStoreInMemory;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.loader.StaticLoader;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.loader.StaticLoaders.ofClassPath;
import static io.datakernel.util.CollectionUtils.list;
import static java.lang.Boolean.parseBoolean;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

//[START EXAMPLE]
public class AuthLauncher extends HttpServerLauncher {
	@Override
	protected Collection<Module> getBusinessLogicModules() {
		return list(new AbstractModule() {
			@Provides
			@Singleton
			AuthService loginService() {
				return new AuthServiceImpl();
			}

			@Provides
			@Singleton
			StaticLoader staticLoader() {
				return ofClassPath(newSingleThreadExecutor(), "site/");
			}

			@Provides
			@Singleton
			AsyncServlet mainServlet(AuthService authService, StaticLoader staticLoader, Eventloop eventloop) {
				SessionStore<String> store = new SessionStoreInMemory<>();
				Supplier<String> sessionIdSupplier = () -> UUID.randomUUID().toString();
				String sessionId = "SESSION_ID";
				return SessionServlet.create(store, sessionId,
						RoutingServlet.create()
								//[START REGION_1]
								.with("/", request -> Promise.of(HttpResponse.redirect302("/login")))
								//[END REGION_1]
								.with(GET, "/signup", SingleResourceStaticServlet.create(eventloop, staticLoader, "signup.html"))
								.with(GET, "/login", SingleResourceStaticServlet.create(eventloop, staticLoader, "login.html"))
								.with(POST, "/login", loadBody()
										.serve(request -> {
											Map<String, String> params = request.getPostParameters();
											String username = params.get("username");
											String password = params.get("password");
											if (authService.authorize(username, password)) {
												String id = sessionIdSupplier.get();

												store.save(id, "My saved object in session");
												return Promise.of(HttpResponse.redirect302("/members")
														.withCookie(HttpCookie.of(sessionId, id)));
											}
											return staticLoader.getResource("errorPage.html")
													.then(body -> Promise.of(HttpResponse.ofCode(404)
															.withBody(body)));
										}))
								.with(POST, "/signup", loadBody()
										.serve(request -> {
											Map<String, String> params = request.getPostParameters();
											String username = params.get("username");
											String password = params.get("password");

											if (username != null && password != null) {
												authService.register(username, password);
											}
											return Promise.of(HttpResponse.redirect302("/login"));
										})),
						RoutingServlet.create()
								//[START REGION_2]
								.with("/", request -> Promise.of(HttpResponse.redirect302("/members")))
								//[END REGION_2]
								//[START REGION_3]
								.with("/members/*", RoutingServlet.create()
										.with(GET, "/", $ -> staticLoader.getResource("index.html")
												.then(body -> Promise.of(HttpResponse.ok200()
														.withBody(body))))
										//[START REGION_4]
										.with(GET, "/cookie", request -> Promise.of(HttpResponse.ok200()
												.withBody(wrapUtf8(request.getAttachment(String.class)))))
										//[END REGION_4]
										.with(POST, "/logout", request -> {
											String id = request.getCookie(sessionId);
											if (id != null) {
												return Promise.of(HttpResponse.redirect302("/")
														.withCookie(HttpCookie.of(sessionId, id)
																.withPath("/")
																.withMaxAge(0)));
											}
											return Promise.of(HttpResponse.ofCode(404));
										}))
						//[END REGION_3]
				);

			}
		});
	}

	public static void main(String[] args) throws Exception {
		AuthLauncher launcher = new AuthLauncher();
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
//[END EXAMPLE]
