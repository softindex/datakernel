package io.datakernel.vlog;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.http.loader.StaticLoader;
import io.datakernel.ot.OTState;
import io.datakernel.remotefs.FsClient;
import io.datakernel.vlog.container.AppUserContainer;
import io.datakernel.vlog.dao.AppDao;
import io.datakernel.vlog.dao.AppDaoImpl;
import io.datakernel.vlog.handler.ProgressListener;
import io.datakernel.vlog.handler.VideoMultipartHandler;
import io.datakernel.vlog.ot.VlogMetadata;
import io.datakernel.vlog.servlets.PublicServlet;
import io.global.api.AppDir;
import io.global.appstore.AppStore;
import io.global.appstore.HttpAppStore;
import io.global.comm.container.CommModule;
import io.global.common.KeyPair;
import io.global.common.SimKey;
import io.global.debug.ObjectDisplayRegistry;
import io.global.fs.local.GlobalFsDriver;
import io.global.mustache.MustacheTemplater;
import io.global.ot.TypedRepoNames;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.ContainerServlet;
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueContainer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.vlog.GlobalVlogApp.DEFAULT_VLOG_FS_DIR;
import static io.datakernel.vlog.util.HttpUtil.renderErrors;
import static io.datakernel.vlog.util.Utils.REGISTRY;
import static io.global.debug.ObjectDisplayRegistryUtils.ts;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.launchers.Initializers.sslServerInitializer;

public class GlobalVlogModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final Path DEFAULT_STATIC_PATH = Paths.get("static/files");
	private final TypedRepoNames repoNames;

	@Override
	protected void configure() {
		install(CommModule.create());

		bind(CodecFactory.class).toInstance(REGISTRY);
		bind(TypedRepoNames.class).toInstance(repoNames);

		bind(AppUserContainer.class).in(ContainerScope.class);
		bind(AppDao.class).to(AppDaoImpl.class).in(ContainerScope.class);
		bind(Key.of(String.class).named(AppDir.class)).toInstance(DEFAULT_VLOG_FS_DIR);
	}

	public GlobalVlogModule(TypedRepoNames repoNames) {
		this.repoNames = repoNames;
	}

	@Provides
	@ContainerScope
	Map<String, ProgressListener> progressListenerMap() {
		return new ConcurrentHashMap<>();
	}

	@Provides
	@ContainerScope
	FsClient fsClient(KeyPair keyPair, GlobalFsDriver fsDriver, @AppDir String appDir) {
		return fsDriver.adapt(keyPair).subfolder(appDir);
	}

	@Provides
	AsyncHttpServer asyncHttpServer(Executor executor, Eventloop eventloop, Config config, ContainerServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(sslServerInitializer(executor, config.getChild("http")));
	}

	@Provides
	AsyncServlet servlet(Config config, AppStore appStore, MustacheTemplater templater,
			StaticLoader staticLoader, Injector injector,
			@Named("debug") @Optional AsyncServlet debugServlet) {
		String appStoreUrl = config.get("appStoreUrl");
		RoutingServlet routingServlet = RoutingServlet.create()
				.map("/*", PublicServlet.create(appStoreUrl, appStore, templater, injector))
				.map("/static/*", StaticServlet.create(staticLoader));
		if (debugServlet != null) {
			routingServlet.map("/debug/*", debugServlet);
		}
		return routingServlet
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
	OTDriver otDriver(GlobalOTNode node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEFAULT_SIM_KEY);
		return new OTDriver(node, simKey);
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	@ContainerScope
	VideoMultipartHandler multipartHandler() {
		return new VideoMultipartHandler();
	}

	@Provides
	@ContainerScope
	OTState<ChangeValue<VlogMetadata>> forumMetadataState() {
		return ChangeValueContainer.of(VlogMetadata.EMPTY);
	}

	@Provides
	@ContainerScope
	ObjectDisplayRegistry diffDisplay() {
		return ObjectDisplayRegistry.create()
				.withDisplay(new Key<ChangeValue<VlogMetadata>>() {},
						($, p) -> p.getNext() == VlogMetadata.EMPTY ? "remove vlog metadata" :
								("set vlog title to '" + p.getNext().getTitle() + "', vlog description to '" + p.getNext().getDescription() + '\''),
						($, p) -> {
							VlogMetadata prev = p.getPrev();
							VlogMetadata next = p.getNext();
							String result = "";
							if (!prev.getTitle().equals(next.getTitle())) {
								result += "change vlog title from '" + prev.getTitle() + "' to '" + next.getTitle() + '\'';
							}
							if (!prev.getDescription().equals(next.getDescription())) {
								if (!result.isEmpty()) {
									result += ", ";
								}
								result += "change vlog description from '" + prev.getDescription() + "' to '" + next.getDescription() + '\'';
							}
							if (result.isEmpty()){
								result += "nothing has been changed";
							}
							result += "" + ts(p.getTimestamp());
							return result;
						});
	}

}
