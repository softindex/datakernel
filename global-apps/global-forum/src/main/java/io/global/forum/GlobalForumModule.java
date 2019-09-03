package io.global.forum;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.http.session.SessionStore;
import io.global.appstore.AppStoreApi;
import io.global.appstore.HttpAppStoreApi;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SimKey;
import io.global.forum.container.ForumRepoNames;
import io.global.forum.container.ForumUserContainer;
import io.global.forum.dao.ForumDao;
import io.global.forum.ot.session.UserIdSessionStore;
import io.global.forum.pojo.UserData;
import io.global.forum.pojo.UserId;
import io.global.forum.pojo.UserRole;
import io.global.fs.local.GlobalFsDriver;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.service.ContainerKeyManager;
import io.global.ot.service.ContainerManager;
import io.global.ot.service.FsContainerKeyManager;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.Utils.generateString;
import static io.global.forum.Utils.templated;
import static io.global.forum.pojo.AuthService.DK_APP_STORE;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;

public final class GlobalForumModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final String SESSION_ID = "SESSION_ID";
	public static final Path DEFAULT_CONTAINERS_DIR = Paths.get("containers");

	private final String forumFsDir;
	private final ForumRepoNames forumRepoNames;

	public GlobalForumModule(String forumFsDir, ForumRepoNames forumRepoNames) {
		this.forumFsDir = forumFsDir;
		this.forumRepoNames = forumRepoNames;
	}

	@Provides
	AsyncHttpServer asyncHttpServer(Eventloop eventloop, Config config, AsyncServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	AsyncServlet servlet(ContainerManager<ForumUserContainer> containerManager, @Named("Forum") AsyncServlet forumServlet) {
		return RoutingServlet.create()
				.map("/:pubKey/*", request -> {
					try {
						PubKey pubKey = PubKey.fromString(request.getPathParameter("pubKey"));
						ForumUserContainer container = containerManager.getUserContainer(pubKey);
						if (container == null) {
							return Promise.of(HttpResponse.notFound404());
						}
						request.attach(container);
						return forumServlet.serve(request);
					} catch (ParseException ignored) {
						return Promise.of(HttpResponse.notFound404());
					}
				});
	}

	@Provides
	AsyncServletDecorator sessionDecorator() {
		return servlet -> request -> {
			String sessionId = request.getCookie(SESSION_ID);
			if (sessionId != null) {
				ForumUserContainer container = request.getAttachment(ForumUserContainer.class);
				String pubKeyString = container.getKeys().getPubKey().asString();
				UserIdSessionStore sessionStore = container.getSessionStore();
				return sessionStore.get(sessionId)
						.then(userId -> {
							if (userId != null) {
								request.attach(userId);
							}
							return servlet.serve(request)
									.map(response -> {
										int maxAge = userId == null ? 0 : (int) sessionStore.getSessionLifetime().getSeconds();
										return response
												.withCookie(HttpCookie.of(SESSION_ID, sessionId)
														.withMaxAge(maxAge)
														.withPath("/" + pubKeyString));
									});
						});
			} else {
				return servlet.serve(request);
			}
		};
	}

	@Provides
	@Named("Forum")
	AsyncServlet forumServlet(Eventloop eventloop, @Named("App store URL") String appStoreUrl, AsyncServletDecorator sessionDecorator) {
		Mustache mustache = new DefaultMustacheFactory(new File("static/templates")).compile("session.mustache");
		AppStoreApi appStoreApi = HttpAppStoreApi.create(appStoreUrl, AsyncHttpClient.create(eventloop));
		return RoutingServlet.create()
				.map(GET, "/", sessionDecorator.serve(request -> {
					Map<String, Object> mustacheContext = map("appStoreUrl", appStoreUrl);
					ForumUserContainer container = request.getAttachment(ForumUserContainer.class);
					UserId userId = request.getAttachment(UserId.class);
					PubKey pubKey = container.getKeys().getPubKey();
					mustacheContext.put("pubKey", pubKey.asString());

					return Promise.complete()
							.then($ -> {
								if (userId != null) {
									mustacheContext.put("userId", userId);
									container.getForumDao().getUser(userId)
											.whenResult(userData -> mustacheContext.put("userData", userData));
								}
								return Promise.complete();
							})
							.map($ -> templated(mustache, mustacheContext));
				}))
				.map(GET, "/auth", request -> {
					String token = request.getQueryParameter("token");
					if (token == null) {
						return Promise.ofException(HttpException.ofCode(400));
					}
					return appStoreApi.exchangeAuthToken(token)
							.then(profile -> {
								ForumUserContainer container = request.getAttachment(ForumUserContainer.class);
								PubKey containerPubKey = container.getKeys().getPubKey();
								PubKey pubKey = profile.getPubKey();
								UserId userId = new UserId(DK_APP_STORE, pubKey.asString());

								String sessionId = generateString(32);

								ForumDao forumDao = container.getForumDao();
								return forumDao
										.getUser(userId)
										.then(existing -> {
											if (existing != null) {
												return Promise.complete();
											}
											UserRole userRole = containerPubKey.equals(pubKey) ?
													UserRole.ADMIN :
													UserRole.COMMON;

											UserData userData = new UserData(userRole, profile.getEmail(),
													profile.getUsername(), profile.getFirstName(), profile.getLastName(), null);
											return forumDao.updateUser(userId, userData);
										})
										.then($ -> {
											UserIdSessionStore sessionStore = container.getSessionStore();
											return sessionStore.save(sessionId, userId)
													.map($2 -> {
														String pubKeyString = container.getKeys().getPubKey().asString();
														return HttpResponse.redirect302('/' + pubKeyString)
																.withCookie(HttpCookie.of(SESSION_ID, sessionId)
																		.withPath("/" + pubKeyString)
																		.withMaxAge((int) sessionStore.getSessionLifetime().getSeconds()));
													});
										});
							});
				})
				.map(POST, "/logout", sessionDecorator.serve(request -> {
					String sessionId = request.getCookie(SESSION_ID);
					if (sessionId == null) {
						return Promise.of(HttpResponse.ok200());
					}
					ForumUserContainer container = request.getAttachment(ForumUserContainer.class);
					SessionStore<UserId> sessionStore = container.getSessionStore();
					return sessionStore.remove(sessionId)
							.map($ -> HttpResponse.redirect302('/' + container.getKeys().getPubKey().asString())
									.withCookie(HttpCookie.of(SESSION_ID, sessionId)
											.withPath("/" + container.getKeys().getPubKey().asString())
											.withMaxAge(0)));
				}));
	}

	@Provides
	ContainerKeyManager containerKeyManager(Eventloop eventloop, Executor executor, Config config) {
		Path containersDir = config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR);
		return FsContainerKeyManager.create(eventloop, executor, containersDir, true);
	}

	@Provides
	ContainerManager<ForumUserContainer> containerHolder(Eventloop eventloop, ContainerKeyManager containerKeyManager, BiFunction<Eventloop, PrivKey, ForumUserContainer> factory) {
		return ContainerManager.create(eventloop, containerKeyManager, factory);
	}

	@Provides
	BiFunction<Eventloop, PrivKey, ForumUserContainer> containerFactory(OTDriver otDriver, GlobalFsDriver fsDriver) {
		return (eventloop, privKey) ->
				ForumUserContainer.create(eventloop, privKey, otDriver, fsDriver.adapt(privKey).subfolder(forumFsDir), forumRepoNames);
	}

	@Provides
	OTDriver otDriver(GlobalOTNode node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEFAULT_SIM_KEY);
		return new OTDriver(node, simKey);
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	@Named("App store URL")
	String appStoreUrl(Config config) {
		return config.get("appStoreUrl");
	}
}
