package io.datakernel.vlog.servlets;

import io.datakernel.http.*;
import io.datakernel.http.session.SessionStore;
import io.datakernel.promise.Promise;
import io.global.appstore.AppStore;
import io.global.comm.dao.CommDao;
import io.global.comm.pojo.UserData;
import io.global.common.PubKey;
import io.global.mustache.MustacheTemplater;
import io.global.ot.session.UserId;

import java.time.Duration;

import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.http.HttpHeaders.REFERER;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.datakernel.vlog.GlobalVlogApp.SESSION_ID;
import static io.global.Utils.generateString;
import static io.global.Utils.isGzipAccepted;
import static io.global.comm.pojo.UserRole.COMMON;
import static io.global.comm.pojo.UserRole.OWNER;
import static io.global.ot.session.AuthService.DK_APP_STORE;

public final class AuthServlet {
	public static AsyncServlet create(AppStore appStore, MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/login", request -> {
					String origin = request.getQueryParameter("origin");
					origin = origin != null ? origin : request.getHeader(REFERER);
					origin = origin != null ? origin : "/";
					return request.getAttachment(UserId.class) != null ?
							Promise.of(redirect302(origin)) :
							templater.render("login",
									map("loginScreen", true,
											"origin", origin),
									isGzipAccepted(request));
				})
				.map(POST, "/logout", request -> {
					String sessionId = request.getCookie(SESSION_ID);
					if (sessionId == null) {
						return Promise.of(HttpResponse.ok200());
					}
					CommDao commDao = request.getAttachment(CommDao.class);
					return commDao.getSessionStore().remove(sessionId)
							.map($ -> HttpResponse.redirect302("/")
									.withCookie(HttpCookie.of(SESSION_ID)
											.withPath("/")
											.withMaxAge(Duration.ZERO)));
				})
				.map(GET, "/authorize", request -> {
					String token = request.getQueryParameter("token");
					if (token == null) {
						return Promise.ofException(HttpException.ofCode(400));
					}
					return appStore.exchangeAuthToken(token).then(profile -> {
						CommDao commDao = request.getAttachment(CommDao.class);
						PubKey containerPubKey = commDao.getKeys().getPubKey();
						PubKey pubKey = profile.getPubKey();
						UserId userId = new UserId(DK_APP_STORE, pubKey.asString());
						String sessionId = generateString(32);
						return commDao.getUsers()
								.get(userId)
								.then(existing -> {
									if (existing != null) {
										return Promise.complete();
									}
									UserData userData = new UserData(
											containerPubKey.equals(pubKey) ? OWNER : COMMON,
											profile.getEmail(), profile.getUsername(),
											profile.getFirstName(), profile.getLastName(), null);
									return commDao.getUsers()
											.put(userId, userData);
								})
								.then($ -> {
									SessionStore<UserId> sessionStore = commDao.getSessionStore();
									return sessionStore.save(sessionId, userId)
											.map($2 -> request.getQueryParameter("origin"))
											.map(origin -> {
														String url = origin != null ? origin : "/";
														return redirect302(url)
																.withCookie(HttpCookie.of(SESSION_ID, sessionId)
																		.withPath("/")
																		.withMaxAge(sessionStore.getSessionLifetime()));
													}
											);
								});
					});
				});
	}
}
