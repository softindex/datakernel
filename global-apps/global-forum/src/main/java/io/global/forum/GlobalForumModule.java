package io.global.forum;

import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.loader.StaticLoader;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SimKey;
import io.global.forum.Utils.MustacheTemplater;
import io.global.forum.container.ForumRepoNames;
import io.global.forum.container.ForumUserContainer;
import io.global.forum.dao.ForumDao;
import io.global.forum.dao.ThreadDao;
import io.global.forum.http.PublicServlet;
import io.global.forum.ot.session.UserIdSessionStore;
import io.global.forum.pojo.AuthService;
import io.global.forum.pojo.ThreadMetadata;
import io.global.forum.pojo.UserId;
import io.global.fs.local.GlobalFsDriver;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.service.ContainerKeyManager;
import io.global.ot.service.ContainerManager;
import io.global.ot.service.FsContainerKeyManager;
import io.global.ot.service.UserContainer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.forum.http.PublicServlet.DATE_TIME_FORMATTER;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;

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

	boolean didIt = false;

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
						ForumDao dao = container.getForumDao();

						// this is a stub ofc
						if (!didIt) {
							didIt = true;
							UserId anton = new UserId(AuthService.DK_APP_STORE, "anton");
							UserId eduard = new UserId(AuthService.DK_APP_STORE, "eduard");
							UserId lera = new UserId(AuthService.DK_APP_STORE, "lera");
							dao.createThread(new ThreadMetadata("thread #1"))
									.then(tid -> {
										ThreadDao threadDao = dao.getThreadDao(tid);
										assert threadDao != null;

										return threadDao.addRootPost(anton, "Hello World", emptyMap())
												.then($ -> threadDao.addPost(eduard, 0L, "Hello, Anton", emptyMap()))
												.then(pid -> threadDao.addPost(anton, pid, "Hello, Eduard", emptyMap()))
												.then($ -> threadDao.addPost(lera, 0L, "Goodbye, Anton", emptyMap()));
									})
									.then($ -> dao.createThread(new ThreadMetadata("second thread")))
									.then(tid -> {
										ThreadDao threadDao = dao.getThreadDao(tid);
										assert threadDao != null;

										return threadDao.addRootPost(anton, "Hello World", emptyMap())
												.then($ -> threadDao.addPost(eduard, 0L, "Hello, Anton", emptyMap()))
												.then(pid -> threadDao.addPost(anton, pid, "Hello, Eduard", emptyMap()))
												.then($ -> threadDao.addPost(lera, 0L, "Goodbye, Anton #1", emptyMap()))
												.then($ -> threadDao.addPost(lera, 0L, "Goodbye, Anton #2", emptyMap()))
												.then($ -> threadDao.addPost(lera, 0L, "Goodbye, Anton #3", emptyMap()))
												.then($ -> threadDao.addPost(lera, 0L, "Goodbye, Anton #4", emptyMap()))
												.then($ -> threadDao.addPost(lera, 0L, "Goodbye, Anton #5", emptyMap()))
												.then($ -> threadDao.addPost(lera, 0L, "Goodbye, Anton #6", emptyMap()))
												.then($ -> threadDao.addPost(lera, 0L, "Goodbye, Anton #7", emptyMap()))
												.then($ -> threadDao.addPost(lera, 0L, "Goodbye, Anton #8", emptyMap()))
												.then($ -> threadDao.addPost(lera, 0L, "Goodbye, Anton #9", emptyMap()));
									})
									.whenComplete(() -> {});
						}

						request.attach(ForumDao.class, dao);
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
	AsyncServlet forumServlet(MustacheTemplater templater, StaticLoader staticLoader) {
		return RoutingServlet.create()
				.map("/*", PublicServlet.create(templater))
				.map("/static/*", StaticServlet.create(staticLoader))
				.then(servlet ->
						request -> {
							Map<String, Object> mustacheContext = templater.getStaticContext();

							mustacheContext.put("pubKey", request.getPathParameter("pubKey"));
							mustacheContext.put("format_date", (Function<String, String>) instant ->
									Instant.ofEpochMilli(Long.parseLong(instant.trim())).atZone(ZoneId.systemDefault()).format(DATE_TIME_FORMATTER));

							return servlet.serve(request);
						})
				.then(servlet ->
						request -> servlet.serve(request)
								.thenEx((response, e) -> {
									if (e instanceof HttpException && ((HttpException) e).getCode() == 404) {
										return templater.render(404, "404", map("message", e.getMessage()));
									}
									if (e != null) {
										return Promise.<HttpResponse>ofException(e);
									}
									if (response.getCode() != 404) {
										return Promise.of(response);
									}
									String message = response.isBodyLoaded() ? response.getBody().asString(UTF_8) : "";
									return templater.render(404, "404", map("message", message.isEmpty() ? null : message));
								}));
	}

	@Provides
	ContainerKeyManager containerKeyManager(Eventloop eventloop, ExecutorService executor, Config config) {
		Path containersDir = config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR);
		return FsContainerKeyManager.create(eventloop, executor, containersDir, true);
	}

	@Provides
	StaticLoader staticLoader(ExecutorService executor, Config config) {
		return StaticLoader.ofPath(executor, Paths.get(config.get("static.files")));
	}

	@Provides
	<T extends UserContainer> ContainerManager<T> containerHolder(Eventloop eventloop, ContainerKeyManager containerKeyManager, BiFunction<Eventloop, PrivKey, T> factory) {
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
	ExecutorService executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	@Named("App store URL")
	String appStoreUrl(Config config) {
		return config.get("appStoreUrl");
	}
}
