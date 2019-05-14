package io.datakernel.examples;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.http.session.InMemorySessionStore;
import io.datakernel.http.session.SessionStore;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.loader.StaticLoader;

import java.util.Collection;
import java.util.UUID;
import java.util.function.Supplier;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.loader.StaticLoaders.ofClassPath;
import static io.datakernel.util.CollectionUtils.list;
import static java.lang.Boolean.parseBoolean;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class AuthLauncher extends HttpServerLauncher {
	@Override
	protected Collection<Module> getBusinessLogicModules() {
		return list(new AbstractModule() {
			@Provides
			@Singleton
			LoginService loginService() {
				return new LoginServiceImpl();
			}

			@Provides
			@Singleton
			StaticLoader staticLoader() {
				return ofClassPath(newSingleThreadExecutor(), "site/");
			}

			@Provides
			@Singleton
			AsyncServlet mainServlet(LoginService loginService, StaticLoader staticLoader, Eventloop eventloop) {
				SessionStore<String> store = new InMemorySessionStore<>();
				Supplier<String> sessionIdSupplier = () -> UUID.randomUUID().toString();
				String sessionId = "SESSION_ID";
				return SessionServlet.create(store, sessionId,
						RoutingServlet.create()
								.with("/", request -> Promise.of(HttpResponse.redirect302("/login")))
								.with(GET, "/signup", SingleResourceStaticServlet.create(eventloop, staticLoader, "signup.html"))
								.with(GET, "/login", SingleResourceStaticServlet.create(eventloop, staticLoader, "login.html"))
								.with(POST, "/login", request -> request.getPostParameters()
										.then(params -> {
											String username = params.get("username");
											String password = params.get("password");
											if (loginService.authorize(username, password)) {
												String id = sessionIdSupplier.get();

												store.save(id, "My saved object in session");
												return Promise.of(HttpResponse.redirect302("/members")
														.withCookie(HttpCookie.of(sessionId, id)));
											}
											return staticLoader.getResource("errorPage.html")
													.then(body -> Promise.of(HttpResponse.ofCode(404)
															.withBody(body)));
										}))
								.with(POST, "/signup", request -> request.getPostParameters()
										.then(params -> {
											String username = params.get("username");
											String password = params.get("password");

											if (username != null && password != null) {
												loginService.register(username, password);
											}
											return Promise.of(HttpResponse.redirect302("/login"));
										})),
						RoutingServlet.create()
								.with("/", request -> Promise.of(HttpResponse.redirect302("/members")))
								.with("/members/*", RoutingServlet.create()
										.with(GET, "/", $ -> staticLoader.getResource("index.html")
												.then(body -> Promise.of(HttpResponse.ok200()
														.withBody(body))))
										.with(GET, "/cookie", request -> Promise.of(HttpResponse.ok200()
												.withBody(wrapUtf8(request.getAttachment(String.class)))))
										.with(POST, "/logout", request -> {
											String id = request.getCookie(sessionId);
											if (id != null) {
												return Promise.of(HttpResponse.redirect302("/")
														.withCookie(HttpCookie.of(sessionId, id)
																));
											}
											return Promise.of(HttpResponse.ofCode(404));
										})));
			}
		});
	}

	public static void main(String[] args) throws Exception {
		AuthLauncher launcher = new AuthLauncher();
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
