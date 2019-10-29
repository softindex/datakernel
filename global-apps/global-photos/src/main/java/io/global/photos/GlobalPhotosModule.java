package io.global.photos;

import io.datakernel.common.MemSize;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.http.loader.StaticLoader;
import io.global.appstore.AppStore;
import io.global.appstore.HttpAppStore;
import io.global.common.PrivKey;
import io.global.common.SimKey;
import io.global.fs.local.GlobalFsDriver;
import io.global.kv.GlobalKvDriver;
import io.global.kv.api.GlobalKvNode;
import io.global.mustache.MustacheTemplater;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.service.ContainerServlet;
import io.global.photos.container.GlobalPhotosContainer;
import io.global.photos.container.RepoNames;
import io.global.photos.dao.AlbumDaoImpl;
import io.global.photos.dao.AlbumDaoImpl.ThumbnailDesc;
import io.global.photos.http.PublicServlet;
import io.global.photos.ot.UserId;

import java.awt.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.photos.util.Utils.REGISTRY;
import static io.global.photos.util.Utils.renderErrors;
import static org.imgscalr.Scalr.Method.SPEED;
import static org.imgscalr.Scalr.Method.ULTRA_QUALITY;

public class GlobalPhotosModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final Path DEFAULT_STATIC_PATH = Paths.get("static/files");

	private final String albumFsDir;
	private final RepoNames forumRepoNames;

	public GlobalPhotosModule(String albumFsDir, RepoNames forumRepoNames) {
		this.albumFsDir = albumFsDir;
		this.forumRepoNames = forumRepoNames;
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
	AsyncHttpServer asyncHttpServer(Config config, Eventloop eventloop, ContainerServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
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
	BiFunction<Eventloop, PrivKey, GlobalPhotosContainer> containerFactory(OTDriver otDriver, GlobalKvDriver<String, UserId> kvDriver,
																		  GlobalFsDriver fsDriver, AlbumDaoImpl.Builder albumBuilder, Executor executor) {
		return (eventloop, privKey) ->
				GlobalPhotosContainer.create(eventloop, privKey, otDriver, kvDriver.adapt(privKey),
						forumRepoNames, albumBuilder, fsDriver.adapt(privKey).subfolder(albumFsDir), executor);
	}

	@Provides
	Map<String, ThumbnailDesc> thumbnailMap(Set<ThumbnailDesc> thumbnailDescs) {
		return thumbnailDescs.stream().collect(Collectors.toMap(ThumbnailDesc::getTitle, o -> o));
	}

	@ProvidesIntoSet
	ThumbnailDesc thumbnail600x600() {
		return ThumbnailDesc.create("base", new Dimension(600, 600), ULTRA_QUALITY);
	}

	public final static String base64Thumbnail = "base64";
	@ProvidesIntoSet
	ThumbnailDesc thumbnail30x30() {
		return ThumbnailDesc.create(base64Thumbnail, new Dimension(30, 30), SPEED);
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
	AlbumDaoImpl.Builder albumDaoBuilder(Config config, Executor executor, Map<String, ThumbnailDesc> thumbnailMap, GlobalFsDriver fsDriver) {
		MemSize memSize = config.get(ofMemSize(), "image.upload.limit");
		return new AlbumDaoImpl.Builder(memSize, (ExecutorService) executor, thumbnailMap);
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}
}
