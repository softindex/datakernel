package io.global.forum;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.http.loader.StaticLoader;
import io.datakernel.promise.Promise;
import io.global.appstore.AppStore;
import io.global.appstore.HttpAppStore;
import io.global.comm.container.CommRepoNames;
import io.global.comm.pojo.UserId;
import io.global.common.PrivKey;
import io.global.common.SimKey;
import io.global.forum.container.ForumUserContainer;
import io.global.forum.http.PublicServlet;
import io.global.fs.local.GlobalFsDriver;
import io.global.kv.GlobalKvDriver;
import io.global.kv.api.GlobalKvNode;
import io.global.mustache.MustacheTemplater;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.service.ContainerServlet;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.forum.util.Utils.REGISTRY;
import static io.global.forum.util.Utils.renderErrors;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;

public final class GlobalForumModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final Path DEFAULT_STATIC_PATH = Paths.get("static/files");

	private final String forumFsDir;
	private final CommRepoNames forumRepoNames;

	public GlobalForumModule(String forumFsDir, CommRepoNames forumRepoNames) {
		this.forumFsDir = forumFsDir;
		this.forumRepoNames = forumRepoNames;
	}

	@Provides
	AsyncHttpServer asyncHttpServer(Eventloop eventloop, Config config, ContainerServlet servlet) {
		AsyncServlet debug = RoutingServlet.create() // TODO anton: this is debug-only
				.map("/*", servlet)
				.map(GET, "/", request ->
						Promise.of(redirect302("/79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798:483ada7726a3c4655da4fbfc0e1108a8fd17b448a68554199c47d08ffb10d4b8")));
		return AsyncHttpServer.create(eventloop, debug)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	AsyncServlet servlet(Config config, AppStore appStore, MustacheTemplater templater, StaticLoader staticLoader) {
		String appStoreUrl = config.get("appStoreUrl");
		return RoutingServlet.create()
				.map("/*", PublicServlet.create(appStoreUrl, appStore, templater))
				.map("/static/*", StaticServlet.create(staticLoader))
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
	BiFunction<Eventloop, PrivKey, ForumUserContainer> containerFactory(OTDriver otDriver, GlobalKvDriver<String, UserId> kvDriver,
			GlobalFsDriver fsDriver) {
		return (eventloop, privKey) ->
				ForumUserContainer.create(eventloop, privKey, otDriver, kvDriver.adapt(privKey), fsDriver.adapt(privKey).subfolder(forumFsDir), forumRepoNames);
	}

	@Provides
	OTDriver otDriver(GlobalOTNode node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEFAULT_SIM_KEY);
		return new OTDriver(node, simKey);
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
