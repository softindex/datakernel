package io.global.blog;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.loader.StaticLoader;
import io.global.appstore.AppStore;
import io.global.appstore.HttpAppStore;
import io.global.comm.container.CommRepoNames;
import io.global.comm.pojo.UserId;
import io.global.common.PrivKey;
import io.global.common.SimKey;
import io.global.blog.container.BlogUserContainer;
import io.global.blog.http.PublicServlet;
import io.global.blog.http.view.PostView;
import io.global.mustache.MustacheTemplater;
import io.global.blog.preprocessor.Preprocessor;
import io.global.fs.local.GlobalFsDriver;
import io.global.kv.GlobalKvDriver;
import io.global.kv.api.GlobalKvNode;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.service.ContainerServlet;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.blog.util.Utils.REGISTRY;
import static io.global.blog.util.Utils.renderErrors;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;

public final class GlobalBlogModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final Path DEFAULT_STATIC_PATH = Paths.get("static/files");

	private final String blogFsDir;
	private final CommRepoNames blogRepoNames;

	@Override
	protected void configure() {
		install(new PreprocessorsModule());
	}

	public GlobalBlogModule(String blogFsDir, CommRepoNames blogRepoNames) {
		this.blogFsDir = blogFsDir;
		this.blogRepoNames = blogRepoNames;
	}

	@Provides
	AsyncHttpServer asyncHttpServer(Eventloop eventloop, Config config, ContainerServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	AsyncServlet servlet(Config config, AppStore appStore, MustacheTemplater templater, StaticLoader staticLoader, Executor executor,
						 @Named("threadList") Preprocessor<PostView> threadListPostViewPreprocessor,
						 @Named("postView") Preprocessor<PostView> postViewPreprocessor,
						 @Named("comments") Preprocessor<PostView> commentsPreprocessor) {
		String appStoreUrl = config.get("appStoreUrl");
		return RoutingServlet.create()
				.map("/*", PublicServlet.create(appStoreUrl, appStore, templater, executor,
						threadListPostViewPreprocessor, postViewPreprocessor, commentsPreprocessor))
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
	BiFunction<Eventloop, PrivKey, BlogUserContainer> containerFactory(OTDriver otDriver, GlobalFsDriver fsDriver, GlobalKvDriver<String, UserId> kvDriver) {
		return (eventloop, privKey) ->
				BlogUserContainer.create(eventloop, privKey, otDriver, kvDriver.adapt(privKey), fsDriver.adapt(privKey).subfolder(blogFsDir), blogRepoNames);
	}

	@Provides
	GlobalKvDriver<String, UserId> kvDriver(GlobalKvNode node) {
		return GlobalKvDriver.create(node, REGISTRY.get(String.class), REGISTRY.get(UserId.class));
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
}
