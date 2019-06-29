import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.*;
import io.datakernel.http.session.SessionServlet;
import io.datakernel.http.session.SessionStore;
import io.datakernel.http.session.SessionStoreInMemory;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.loader.StaticLoader;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

//[START EXAMPLE]
public final class AuthLauncher extends HttpServerLauncher {
	public static final String SESSION_ID = "SESSION_ID";

	@Provides
	AuthService loginService() {
		return new AuthServiceImpl();
	}

	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	@Provides
	private StaticLoader staticLoader(Executor executor) {
		return StaticLoader.ofClassPath(executor, "site/");
	}

	@Provides
	SessionStore<String> sessionStore() {
		return new SessionStoreInMemory<>();
	}

	@Provides
	AsyncServlet servlet(Executor executor, SessionStore<String> sessionStore,
			@Named("public") AsyncServlet publicServlet, @Named("private") AsyncServlet privateServlet) {
		return SessionServlet.create(sessionStore, SESSION_ID, publicServlet, privateServlet);
	}

	@Provides
	@Named("public")
	AsyncServlet publicServlet(AuthService authService, SessionStore<String> store, StaticLoader staticLoader) {
		return RoutingServlet.create()
				.with("/", request -> Promise.of(HttpResponse.redirect302("/login")))
				.with(GET, "/signup", StaticServlet.create(staticLoader, "signup.html"))
				.with(GET, "/login", StaticServlet.create(staticLoader, "login.html"))
				.with(POST, "/login", loadBody()
						.serveFirstSuccessful(
								request -> {
									Map<String, String> params = request.getPostParameters();
									String username = params.get("username");
									String password = params.get("password");
									if (authService.authorize(username, password)) {
										String sessionId = UUID.randomUUID().toString();

										store.save(sessionId, "My object saved in session");
										return Promise.of(HttpResponse.redirect302("/members")
												.withCookie(HttpCookie.of(SESSION_ID, sessionId)));
									}
									return AsyncServlet.NEXT;
								},
								StaticServlet.create(staticLoader, "errorPage.html")))
				.with(POST, "/signup", loadBody()
						.serve(request -> {
							Map<String, String> params = request.getPostParameters();
							String username = params.get("username");
							String password = params.get("password");

							if (username != null && password != null) {
								authService.register(username, password);
							}
							return Promise.of(HttpResponse.redirect302("/login"));
						}));
	}

	@Provides
	@Named("private")
	AsyncServlet privateServlet(StaticLoader staticLoader) {
		return RoutingServlet.create()
				.with("/", request -> Promise.of(HttpResponse.redirect302("/members")))
				.with("/members/*", RoutingServlet.create()
						.with(GET, "/", StaticServlet.create(staticLoader, "index.html"))
						.with(GET, "/cookie", request -> Promise.of(HttpResponse.ok200()
								.withBody(wrapUtf8(request.getAttachment(String.class)))))
						.with(POST, "/logout", request -> {
							String id = request.getCookie(SESSION_ID);
							if (id != null) {
								return Promise.of(HttpResponse.redirect302("/")
										.withCookie(HttpCookie.of(SESSION_ID, id)
												.withPath("/")
												.withMaxAge(0)));
							}
							return Promise.of(HttpResponse.ofCode(404));
						}));
	}

	public static void main(String[] args) throws Exception {
		AuthLauncher launcher = new AuthLauncher();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
