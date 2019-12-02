package io.global.blog;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Eager;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.http.loader.StaticLoader;
import io.datakernel.remotefs.FsClient;
import io.global.appstore.AppStore;
import io.global.appstore.HttpAppStore;
import io.global.blog.container.BlogUserContainer;
import io.global.blog.dao.BlogDao;
import io.global.blog.dao.BlogDaoImpl;
import io.global.blog.http.PublicServlet;
import io.global.blog.http.view.PostView;
import io.global.blog.interceptors.Preprocessor;
import io.global.blog.util.Utils;
import io.global.comm.container.CommModule;
import io.global.ot.TypedRepoNames;
import io.global.common.KeyPair;
import io.global.common.SimKey;
import io.global.fs.local.GlobalFsDriver;
import io.global.kv.GlobalKvDriver;
import io.global.kv.api.KvClient;
import io.global.mustache.MustacheTemplater;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.ContainerServlet;
import io.global.ot.session.UserId;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.global.blog.util.Utils.renderErrors;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.launchers.Initializers.sslServerInitializer;

public final class GlobalBlogModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final Path DEFAULT_STATIC_PATH = Paths.get("static/files");

	private final String blogFsDir;
	private final TypedRepoNames blogRepoNames;

	@Override
	protected void configure() {
		install(new PreprocessorsModule());
		install(CommModule.create());

		bind(CodecFactory.class).toInstance(Utils.REGISTRY);
		bind(TypedRepoNames.class).toInstance(blogRepoNames);

		bind(BlogUserContainer.class).in(ContainerScope.class);
		bind(BlogDao.class).to(BlogDaoImpl.class).in(ContainerScope.class);
	}

	public GlobalBlogModule(String blogFsDir, TypedRepoNames blogRepoNames) {
		this.blogFsDir = blogFsDir;
		this.blogRepoNames = blogRepoNames;
	}

	@Provides
	@ContainerScope
	FsClient fsClient(KeyPair keyPair, GlobalFsDriver fsDriver) {
		return fsDriver.adapt(keyPair).subfolder(blogFsDir);
	}

	@Provides
	@ContainerScope
	KvClient<String, UserId> kvClient(KeyPair keyPair, GlobalKvDriver<String, UserId> kvDriver) {
		return kvDriver.adapt(keyPair);
	}

	@Provides
	AsyncHttpServer asyncHttpServer(Eventloop eventloop, Executor executor, Config config, ContainerServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(sslServerInitializer(executor, config.getChild("http")));
	}

	@Provides
	AsyncServlet servlet(Config config, AppStore appStore, MustacheTemplater templater, StaticLoader staticLoader, Executor executor,
						 @Named("threadList") Preprocessor<PostView> threadListPostViewPreprocessor,
						 @Named("postView") Preprocessor<PostView> postViewPreprocessor) {
		String appStoreUrl = config.get("appStoreUrl");
		return RoutingServlet.create()
				.map("/*", PublicServlet.create(appStoreUrl, appStore, templater, executor,
						threadListPostViewPreprocessor, postViewPreprocessor))
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
	@Eager
	OTDriver otDriver(GlobalOTNode node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEFAULT_SIM_KEY);
		return new OTDriver(node, simKey);
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}
}
