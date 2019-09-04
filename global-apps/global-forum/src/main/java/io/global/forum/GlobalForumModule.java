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
import io.global.appstore.AppStore;
import io.global.appstore.HttpAppStore;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SimKey;
import io.global.forum.container.ForumRepoNames;
import io.global.forum.container.ForumUserContainer;
import io.global.forum.dao.ForumDao;
import io.global.forum.dao.ThreadDao;
import io.global.forum.http.PublicServlet;
import io.global.forum.pojo.*;
import io.global.forum.util.MustacheTemplater;
import io.global.fs.local.GlobalFsDriver;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.service.ContainerKeyManager;
import io.global.ot.service.ContainerManager;
import io.global.ot.service.FsContainerKeyManager;
import io.global.ot.service.UserContainer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static java.util.Collections.emptyMap;

public final class GlobalForumModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final Path DEFAULT_CONTAINERS_DIR = Paths.get("containers");
	public static final Path DEFAULT_STATIC_PATH = Paths.get("static/files");

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

						// region stub data
						if (!didIt) {
							didIt = true;
							UserId anton = new UserId(AuthService.DK_APP_STORE, "anton");
							UserId eduard = new UserId(AuthService.DK_APP_STORE, "eduard");
							UserId lera = new UserId(AuthService.DK_APP_STORE, "lera");

							dao.updateUser(anton, new UserData(UserRole.COMMON, "", "Anton", null, null));
							dao.updateUser(lera, new UserData(UserRole.COMMON, "", "Lera", null, null));
							dao.updateUser(eduard, new UserData(UserRole.COMMON, "", "Eduard", null, null));

							dao.createThread(new ThreadMetadata("thread #1"))
									.then(tid -> {
										ThreadDao threadDao = dao.getThreadDao(tid);
										assert threadDao != null;

										return threadDao.addRootPost(anton, "Hello World", emptyMap())
												.then($ -> threadDao.addPost(eduard, "root", "Hello, Anton", emptyMap()))
												.then(pid -> threadDao.addPost(anton, pid, "Hello, Eduard", emptyMap()))
												.then($ -> threadDao.addPost(lera, "root", "Goodbye, Anton", emptyMap()));
									})
									.then($ -> dao.createThread(new ThreadMetadata("second thread")))
									.then(tid -> {
										ThreadDao threadDao = dao.getThreadDao(tid);
										assert threadDao != null;

										return threadDao.addRootPost(anton, "Hello World", emptyMap())
												.then($ -> threadDao.addPost(eduard, "root", "Hello, Anton", emptyMap()))
												.then(pid -> threadDao.addPost(anton, pid, "Hello, Eduard", emptyMap()))
												.then($ -> threadDao.addPost(lera, "root", "Goodbye, Anton #1", emptyMap()))
												.then($ -> threadDao.addPost(lera, "root", "Goodbye, Anton #2", emptyMap()))
												.then($ -> threadDao.addPost(lera, "root", "Goodbye, Anton #3", emptyMap()))
												.then($ -> threadDao.addPost(lera, "root", "Goodbye, Anton #4", emptyMap()))
												.then($ -> threadDao.addPost(lera, "root", "Goodbye, Anton #5", emptyMap()))
												.then($ -> threadDao.addPost(lera, "root", "Goodbye, Anton #6", emptyMap()))
												.then($ -> threadDao.addPost(lera, "root", "Goodbye, Anton #7", emptyMap()))
												.then($ -> threadDao.addPost(lera, "root", "Goodbye, Anton #8", emptyMap()))
												.then($ -> threadDao.addPost(lera, "root", "Goodbye, Anton #9", emptyMap()));
									})
									.whenResult($ -> {});
						}
						// endregion

						request.attach(ForumDao.class, dao);
						return forumServlet.serve(request);
					} catch (ParseException ignored) {
						return Promise.of(HttpResponse.notFound404());
					}
				})
				.map(GET, "/", request -> {
					try {
						return Promise.of(HttpResponse.redirect302(PrivKey.fromString("1").computePubKey().asString() + "/"));
					} catch (ParseException e) {
						throw new AssertionError();
					}
				}); // TODO anton: this is debug-only
	}

	@Provides
	@Named("Forum")
	AsyncServlet forumServlet(Config config, AppStore appStore, MustacheTemplater templater, StaticLoader staticLoader) {
		String appStoreUrl = config.get("appStoreUrl", "");
		return RoutingServlet.create()
				.map("/*", PublicServlet.create(appStore, templater)
						.then(servlet -> request -> {
							templater.put("appStoreUrl", appStoreUrl);
							return servlet.serve(request);
						}))
				.map("/static/*", StaticServlet.create(staticLoader));
	}

	@Provides
	AppStore appStore(Config config, Eventloop eventloop) {
		return HttpAppStore.create(config.get("appStoreUrl"), AsyncHttpClient.create(eventloop));
	}

	@Provides
	ContainerKeyManager containerKeyManager(Eventloop eventloop, ExecutorService executor, Config config) {
		Path containersDir = config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR);
		return FsContainerKeyManager.create(eventloop, executor, containersDir, true);
	}

	@Provides
	StaticLoader staticLoader(ExecutorService executor, Config config) {
		Path staticPath = config.get(ofPath(), "static.files", DEFAULT_STATIC_PATH);
		return StaticLoader.ofPath(executor, staticPath);
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
