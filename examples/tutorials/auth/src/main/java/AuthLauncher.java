import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.*;
import io.datakernel.http.loader.StaticLoader;
import io.datakernel.http.session.SessionServlet;
import io.datakernel.http.session.SessionStore;
import io.datakernel.http.session.SessionStoreInMemory;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.promise.Promise;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

//[START REGION_1]
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
	AsyncServlet servlet(SessionStore<String> sessionStore,
			@Named("public") AsyncServlet publicServlet, @Named("private") AsyncServlet privateServlet) {
		return SessionServlet.create(sessionStore, SESSION_ID, publicServlet, privateServlet);
	}
	//[END REGION_1]

	//[START REGION_2]
	@Provides
	@Named("public")
	AsyncServlet publicServlet(AuthService authService, SessionStore<String> store, StaticLoader staticLoader) {
		return RoutingServlet.create()
				//[START REGION_3]
				.map("/", request -> Promise.of(HttpResponse.redirect302("/login")))
				//[END REGION_3]
				.map(GET, "/signup", StaticServlet.create(staticLoader, "signup.html"))
				.map(GET, "/login", StaticServlet.create(staticLoader, "login.html"))
				//[START REGION_4]
				.map(POST, "/login", loadBody()
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
				//[END REGION_4]
				.map(POST, "/signup", loadBody()
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
	//[END REGION_2]

	//[START REGION_5]
	@Provides
	@Named("private")
	AsyncServlet privateServlet(StaticLoader staticLoader) {
		return RoutingServlet.create()
				//[START REGION_6]
				.map("/", request -> Promise.of(HttpResponse.redirect302("/members")))
				//[END REGION_6]
				//[START REGION_7]
				.map("/members/*", RoutingServlet.create()
						.map(GET, "/", StaticServlet.create(staticLoader, "index.html"))
						//[START REGION_8]
						.map(GET, "/cookie", request -> Promise.of(
								HttpResponse.ok200()
										.withBody(wrapUtf8(request.getAttachment(String.class)))))
						//[END REGION_8]
						.map(POST, "/logout", request -> {
							String id = request.getCookie(SESSION_ID);
							if (id != null) {
								return Promise.of(
										HttpResponse.redirect302("/")
												.withCookie(HttpCookie.of(SESSION_ID, id).withPath("/").withMaxAge(0)));
							}
							return Promise.of(HttpResponse.ofCode(404));
						}));
				//[END REGION_7]
	}
	//[END REGION_5]

	//[START REGION_9]
	public static void main(String[] args) throws Exception {
		AuthLauncher launcher = new AuthLauncher();
		launcher.launch(args);
	}
	//[END REGION_9]
}
