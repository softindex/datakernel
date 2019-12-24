package io.global.ot.session;

import io.datakernel.common.parse.ParseException;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.http.*;
import io.datakernel.http.session.SessionStore;
import io.datakernel.promise.Promise;
import io.global.appstore.AppStore;
import io.global.appstore.HttpAppStore;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.ot.service.UserContainer;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;

import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.Utils.generateString;
import static io.global.ot.session.AuthService.DK_APP_STORE;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class AuthModule extends AbstractModule {
	private final String sessionId;

	public AuthModule(String sessionId) {
		this.sessionId = sessionId;
	}

	@Provides
	@Named("authorization")
	RoutingServlet authorizationServlet(AppStore appStore) {
		return RoutingServlet.create()
				.map(GET, "/auth", request -> {
					String token = request.getQueryParameter("token");
					if (token == null) {
						return Promise.ofException(HttpException.ofCode(400));
					}
					return appStore.exchangeAuthToken(token)
							.then(profile -> {
								PubKey pubKey = profile.getPubKey();
								return authByPubKey(request, pubKey);
							});
				})
				.map(POST, "/authByKey", loadBody()
						.serve(request -> {
							String keyString = request.getBody().asString(UTF_8);
							if (keyString.isEmpty()) {
								return Promise.ofException(HttpException.ofCode(400));
							}
							try {
								PubKey pubKey = PrivKey.fromString(keyString).computePubKey();
								return authByPubKey(request, pubKey);
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(POST, "/logout", request -> {
					String sessionString = request.getCookie(sessionId);
					if (sessionString == null) {
						return Promise.of(HttpResponse.ok200());
					}
					UserContainer userContainer = request.getAttachment(UserContainer.class);
					return userContainer
							.getSessionStore()
							.remove(sessionString)
							.map($ -> HttpResponse.ok200()
									.withCookie(HttpCookie.of(sessionId, sessionString)
											.withPath("/")
											.withMaxAge(Duration.ZERO)));
				});
	}

	@Provides
	@Named("session")
	AsyncServletDecorator sessionDecorator() {
		return servlet ->
				request -> {
					String sessionString = request.getCookie(sessionId);
					if (sessionString == null) {
						return Promise.of(HttpResponse.ofCode(401));
					}
					UserContainer userContainer = request.getAttachment(UserContainer.class);
					return userContainer.getSessionStore().get(sessionString)
							.then(userId -> {
								KeyPair keys = userContainer.getKeys();
								String containerId = keys.getPubKey().asString();
								if (userId == null || !userId.getAuthId().equals(containerId)) {
									return Promise.of(HttpResponse.ofCode(401)
											.withCookie(HttpCookie.of(sessionId, sessionString)
													.withPath("/")
													.withMaxAge(Duration.ZERO)));
								}
								request.attach(keys.getPubKey());
								request.attach(keys);
								return servlet.serveAsync(request);
							});
				};
	}

	@Provides
	AppStore appStore(Config config, IAsyncHttpClient httpClient) {
		return HttpAppStore.create(config.get("appStoreUrl"), httpClient);
	}

	@NotNull
	private Promise<HttpResponse> authByPubKey(@NotNull HttpRequest request, PubKey pubKey) {
		UserContainer userContainer = request.getAttachment(UserContainer.class);
		if (!pubKey.equals(userContainer.getKeys().getPubKey())) {
			return Promise.of(HttpResponse.ofCode(401));
		}
		SessionStore<UserId> sessionStore = userContainer.getSessionStore();
		String pubKeyString = pubKey.asString();
		UserId userId = new UserId(DK_APP_STORE, pubKeyString);
		String sessionString = generateString(32);
		return sessionStore.save(sessionString, userId)
				.map($2 -> {
					HttpCookie sessionCookie = HttpCookie.of(sessionId, sessionString)
							.withPath("/");
					Duration lifetimeHint = sessionStore.getSessionLifetimeHint();
					if (lifetimeHint != null) {
						sessionCookie.setMaxAge(lifetimeHint);
					}
					return HttpResponse.ok200()
							.withPlainText(pubKeyString)
							.withCookie(sessionCookie);
				});
	}
}
