package io.global.forum;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.EagerSingleton;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.loader.StaticLoader;
import io.datakernel.ot.OTSystem;
import io.datakernel.remotefs.FsClient;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.Initializer;
import io.global.appstore.AppStore;
import io.global.appstore.HttpAppStore;
import io.global.comm.container.CommModule;
import io.global.comm.container.TypedRepoNames;
import io.global.comm.pojo.UserId;
import io.global.common.KeyPair;
import io.global.common.SimKey;
import io.global.forum.container.ForumUserContainer;
import io.global.forum.dao.ForumDao;
import io.global.forum.dao.ForumDaoImpl;
import io.global.forum.http.PublicServlet;
import io.global.forum.ot.ForumMetadata;
import io.global.fs.local.GlobalFsDriver;
import io.global.kv.GlobalKvDriver;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.KvClient;
import io.global.mustache.MustacheTemplater;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.service.ContainerManager;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.ContainerServlet;
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueOTSystem;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.forum.util.Utils.REGISTRY;
import static io.global.forum.util.Utils.renderErrors;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;

public final class GlobalForumModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final Path DEFAULT_STATIC_PATH = Paths.get("static/files");

	private final String forumFsDir;
	private final TypedRepoNames forumRepoNames;

	public GlobalForumModule(String forumFsDir, TypedRepoNames forumRepoNames) {
		this.forumFsDir = forumFsDir;
		this.forumRepoNames = forumRepoNames;
	}

	@Override
	protected void configure() {
		install(CommModule.create());

		bind(CodecFactory.class).toInstance(REGISTRY);
		bind(TypedRepoNames.class).toInstance(forumRepoNames);

		bind(ForumUserContainer.class).in(ContainerScope.class);
		bind(ForumDao.class).to(ForumDaoImpl.class).in(ContainerScope.class);
	}

	@Provides
	@ContainerScope
	FsClient fsClient(KeyPair keyPair, GlobalFsDriver fsDriver) {
		return fsDriver.adapt(keyPair).subfolder(forumFsDir);
	}

	@Provides
	@ContainerScope
	KvClient<String, UserId> kvClient(KeyPair keyPair, GlobalKvDriver<String, UserId> kvDriver) {
		return kvDriver.adapt(keyPair);
	}

	@Provides
	AsyncHttpServer asyncHttpServer(Eventloop eventloop, Config config, ContainerServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	AsyncServlet servlet(Config config, AppStore appStore, MustacheTemplater templater, StaticLoader staticLoader) {
		String appStoreUrl = config.get("appStoreUrl");
		return RoutingServlet.create()
				.map("/*", PublicServlet.create(appStoreUrl, appStore, templater))
				.map("/static/*", StaticServlet.create(staticLoader).withHttpCacheMaxAge(31536000))
				.then(renderErrors(templater));
	}

	@Provides
	AppStore appStore(Config config, IAsyncHttpClient httpClient) {
		return HttpAppStore.create(config.get("appStoreUrl"), httpClient);
	}

	@Provides
	StaticLoader staticLoader(Executor executor, Config config) {
		Path staticPath = config.get(ofPath(), "static.files", DEFAULT_STATIC_PATH);
		return StaticLoader.ofPath(executor, staticPath);
	}

	@Provides
	@EagerSingleton
	OTDriver otDriver(GlobalOTNode node, Config config) {
		return new OTDriver(node, config.get(ofSimKey(), "credentials.simKey", DEFAULT_SIM_KEY));
	}

	@Provides
	GlobalKvDriver<String, UserId> kvDriver(GlobalKvNode node) {
		return GlobalKvDriver.create(node, REGISTRY.get(String.class), REGISTRY.get(UserId.class));
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}
}
