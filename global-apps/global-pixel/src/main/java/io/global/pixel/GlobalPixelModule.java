package io.global.pixel;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.loader.StaticLoader;
import io.datakernel.util.MemSize;
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
import io.global.pixel.container.GlobalPixelContainer;
import io.global.pixel.container.RepoNames;
import io.global.pixel.dao.AlbumDaoImpl;
import io.global.pixel.dao.AlbumDaoImpl.Thumbnail;
import io.global.pixel.http.PublicServlet;
import io.global.pixel.ot.UserId;
import org.imgscalr.Scalr;

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
import static io.global.pixel.util.Utils.REGISTRY;
import static io.global.pixel.util.Utils.renderErrors;

public class GlobalPixelModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final Path DEFAULT_STATIC_PATH = Paths.get("static/files");

	private final String albumFsDir;
	private final RepoNames forumRepoNames;

	public GlobalPixelModule(String albumFsDir, RepoNames forumRepoNames) {
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
	BiFunction<Eventloop, PrivKey, GlobalPixelContainer> containerFactory(OTDriver otDriver, GlobalKvDriver<String, UserId> kvDriver,
																		  GlobalFsDriver fsDriver, AlbumDaoImpl.Builder albumBuilder) {
		return (eventloop, privKey) ->
				GlobalPixelContainer.create(eventloop, privKey, otDriver, kvDriver.adapt(privKey), forumRepoNames, albumBuilder, fsDriver.adapt(privKey).subfolder(albumFsDir));
	}

	@Provides
	Map<String, Thumbnail> thumbnailMap(Set<Thumbnail> thumbnails) {
		return thumbnails.stream().collect(Collectors.toMap(Thumbnail::getTitle, o -> o));
	}

	@ProvidesIntoSet
	Thumbnail thumbnail300x300() {
		return Thumbnail.create("base", new Dimension(300, 300), Scalr.Method.ULTRA_QUALITY);
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
	AlbumDaoImpl.Builder albumDaoBuilder(Config config, Executor executor, Map<String, Thumbnail> thumbnailMap, GlobalFsDriver fsDriver) {
		Integer size = config.get(ofInteger(), "image.upload.limit");
		return new AlbumDaoImpl.Builder(MemSize.megabytes(size), (ExecutorService) executor, thumbnailMap);
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}
}
