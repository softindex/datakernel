package io.datakernel.examples;

import io.datakernel.async.Promise;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.http.*;
import io.datakernel.http.session.SessionServlet;
import io.datakernel.http.session.SessionStore;
import io.datakernel.http.session.SessionStoreInMemory;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.loader.StaticLoader;

import java.util.Map;
import java.util.UUID;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;

//[START EXAMPLE]
public class AuthLauncher extends HttpServerLauncher {
	public static final String SESSION_ID = "SESSION_ID";

	@Override
	protected Module getBusinessLogicModule() {
		return new AbstractModule() {
			@Provides
			AuthService loginService() {
				return new AuthServiceImpl();
			}

			@Provides
			AsyncServlet mainServlet(AuthService authService) {
				SessionStore<String> store = new SessionStoreInMemory<>();
				StaticLoader staticLoader = StaticLoader.ofClassPath("site/");
				return SessionServlet.create(store, SESSION_ID,
						RoutingServlet.create()
								//[START REGION_1]
								.with("/", request -> Promise.of(HttpResponse.redirect302("/login")))
								//[END REGION_1]
								.with(GET, "/signup", StaticServlet.create(staticLoader).withMappingTo("signup.html"))
								.with(GET, "/login", StaticServlet.create(staticLoader).withMappingTo("login.html"))
								.with(POST, "/login", loadBody()
										.serve(request -> {
											Map<String, String> params = request.getPostParameters();
											String username = params.get("username");
											String password = params.get("password");
											if (authService.authorize(username, password)) {
												String sessionId = UUID.randomUUID().toString();

												store.save(sessionId, "My saved object in session");
												return Promise.of(HttpResponse.redirect302("/members")
														.withCookie(HttpCookie.of(SESSION_ID, sessionId)));
											}
											return staticLoader.load("errorPage.html")
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
										.with(GET, "/", $ -> staticLoader.load("index.html")
												.then(body -> Promise.of(HttpResponse.ok200()
														.withBody(body))))
										//[START REGION_4]
										.with(GET, "/cookie", request -> Promise.of(HttpResponse.ok200()
												.withBody(wrapUtf8(request.getAttachment(String.class)))))
										//[END REGION_4]
										.with(POST, "/logout", request -> {
											String id = request.getCookie(SESSION_ID);
											if (id != null) {
												return Promise.of(HttpResponse.redirect302("/")
														.withCookie(HttpCookie.of(SESSION_ID, id)
																.withPath("/")
																.withMaxAge(0)));
											}
											return Promise.of(HttpResponse.ofCode(404));
										}))
						//[END REGION_3]
				);

			}
		};
	}

	public static void main(String[] args) throws Exception {
		AuthLauncher launcher = new AuthLauncher();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
