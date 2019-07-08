package io.global.video;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.loader.StaticLoader;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.common.SimKey;
import io.global.fs.http.GlobalFsDriverServlet;
import io.global.fs.local.GlobalFsDriver;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.MapModule;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.map.MapOperation;

import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.object;
import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.common.BinaryDataFormats.createGlobal;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;

public final class GlobalVideoApp extends Launcher {
	public static final String PROPERTIES_FILE = "global-video-app.properties";
	public static final String DEFAULT_SERVER_ID = "Global Video App";
	public static final String DEFAULT_FS_STORAGE = System.getProperty("java.io.tmpdir") + '/' + "global-fs";
	public static final String DEFAULT_STATIC_PATH = "front/build";
	public static final String DEFAULT_LISTEN_ADDRESS = "8080";
	public static final String DEFAULT_METADATA_REPO = "videos/metadata";
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});

	@Inject
	AsyncHttpServer server;

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	CodecFactory codecs() {
		return createGlobal()
				.with(VideoMetadata.class, object(VideoMetadata::new,
						"title", VideoMetadata::getTitle, STRING_CODEC,
						"description", VideoMetadata::getDescription, STRING_CODEC,
						"extension", VideoMetadata::getExtension, STRING_CODEC));
	}

	@Provides
	AsyncServlet servlet(GlobalFsDriver driver, StaticLoader resourceLoader, DynamicOTNodeServlet<MapOperation<String, VideoMetadata>> metadataServlet) {
		return GlobalFsDriverServlet.create(driver)
				.map("/ot/metadata/*", metadataServlet)
				.map(GET, "/*", StaticServlet.create(resourceLoader)
						.withMappingNotFoundTo("index.html"));
	}

	@Provides
	StaticLoader staticLoader(Config config, Executor executor) {
		return StaticLoader.ofPath(executor, Paths.get(config.get("app.http.staticPath")));
	}

	@Provides
	AsyncHttpServer asyncHttpServer(Eventloop eventloop, Config config, AsyncServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("app.http")));
	}

	@Provides
	OTDriver provideDriver(Eventloop eventloop, GlobalOTNode node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEFAULT_SIM_KEY);
		return new OTDriver(node, simKey);
	}

	@Provides
	Comparator<VideoMetadata> comparator() {
		return Comparator
				.comparing(VideoMetadata::getTitle)
				.thenComparing(VideoMetadata::getDescription)
				.thenComparing(VideoMetadata::getExtension);
	}

	@Provides
	Config config() {
		return Config.create()
				.with("node.serverId", DEFAULT_SERVER_ID)
				.with("fs.storage", DEFAULT_FS_STORAGE)
				.with("app.http.staticPath", DEFAULT_STATIC_PATH)
				.with("app.http.listenAddresses", DEFAULT_LISTEN_ADDRESS)
				.overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(ofProperties(System.getProperties()).getChild("config"));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				JmxModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				new MapModule<String, VideoMetadata>(DEFAULT_METADATA_REPO) {},
				new GlobalNodesModule()
						.overrideWith(new LocalNodeCommonModule(DEFAULT_SERVER_ID))
		);
	}

	@Override
	protected void run() throws Exception {
		logger.info("HTTP Server is now available at " + String.join(", ", server.getHttpAddresses()));
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalVideoApp().launch(args);
	}
}
