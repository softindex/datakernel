package io.global.photos;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.common.MemSize;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.*;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.http.loader.StaticLoader;
import io.datakernel.ot.OTState;
import io.datakernel.ot.OTSystem;
import io.datakernel.remotefs.FsClient;
import io.global.api.AppDir;
import io.global.appstore.AppStore;
import io.global.appstore.HttpAppStore;
import io.global.common.KeyPair;
import io.global.common.SimKey;
import io.global.debug.ObjectDisplayRegistry;
import io.global.fs.local.GlobalFsAdapter;
import io.global.fs.local.GlobalFsDriver;
import io.global.kv.GlobalKvDriver;
import io.global.kv.api.GlobalKvNode;
import io.global.mustache.MustacheTemplater;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.ContainerServlet;
import io.global.ot.session.UserId;
import io.global.photos.container.GlobalPhotosContainer;
import io.global.photos.dao.AlbumDaoImpl;
import io.global.photos.dao.AlbumDaoImpl.ThumbnailDesc;
import io.global.photos.dao.MainDao;
import io.global.photos.dao.MainDaoImpl;
import io.global.photos.http.PublicServlet;
import io.global.photos.ot.Album;
import io.global.photos.ot.AlbumOtState;
import io.global.photos.ot.AlbumOtSystem;
import io.global.photos.ot.Photo;
import io.global.photos.ot.operation.*;

import java.awt.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.datakernel.config.ConfigConverters.*;
import static io.global.debug.ObjectDisplayRegistryUtils.*;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.launchers.Initializers.sslServerInitializer;
import static io.global.photos.GlobalPhotosApp.DEFAULT_PHOTOS_FS_DIR;
import static io.global.photos.util.Utils.REGISTRY;
import static io.global.photos.util.Utils.renderErrors;
import static org.imgscalr.Scalr.Method.SPEED;
import static org.imgscalr.Scalr.Method.ULTRA_QUALITY;

public class GlobalPhotosModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final Path DEFAULT_STATIC_PATH = Paths.get("static/files");

	@Override
	protected void configure() {
		bind(GlobalPhotosContainer.class).in(ContainerScope.class);
		bind(MainDao.class).to(MainDaoImpl.class).in(ContainerScope.class);
		bind(CodecFactory.class).toInstance(REGISTRY);
		bind(new Key<OTState<AlbumOperation>>() {}).to(AlbumOtState.class).in(ContainerScope.class);
		bind(Key.of(String.class).named(AppDir.class)).toInstance(DEFAULT_PHOTOS_FS_DIR);
	}

	@Provides
	AsyncServlet servlet(Config config, AppStore appStore, MustacheTemplater templater, StaticLoader staticLoader,
			@Optional @Named("debug") AsyncServlet debugServlet) {
		String appStoreUrl = config.get("appStoreUrl");
		RoutingServlet routingServlet = RoutingServlet.create()
				.map("/*", PublicServlet.create(appStoreUrl, appStore, templater))
				.map("/static/*", StaticServlet.create(staticLoader).withHttpCacheMaxAge(31536000));
		if (debugServlet != null) {
			routingServlet.map("/debug/*", debugServlet);
		}
		return routingServlet
				.then(renderErrors(templater));
	}

	@Provides
	AsyncHttpServer asyncHttpServer(Config config, Executor executor, Eventloop eventloop, ContainerServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(sslServerInitializer(executor, config.getChild("http")));
	}

	@Provides
	StaticLoader staticLoader(Executor executor, Config config) {
		Path staticPath = config.get(ofPath(), "static.files", DEFAULT_STATIC_PATH);
		return StaticLoader.ofPath(executor, staticPath);
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
	ThumbnailDesc thumbnail10x10() {
		return ThumbnailDesc.create(base64Thumbnail, new Dimension(10, 10), SPEED);
	}

	@Provides
	@Named("photoHandler")
	Executor executor() {
		return Executors.newCachedThreadPool();
	}

	@Provides
	@Eager
	AlbumDaoImpl.Builder albumDaoBuilder(Config config, @Named("photoHandler") Executor executor, Map<String, ThumbnailDesc> thumbnailMap, GlobalFsDriver fsDriver) {
		MemSize memSize = config.get(ofMemSize(), "image.upload.limit");
		return new AlbumDaoImpl.Builder(memSize, (ExecutorService) executor, thumbnailMap);
	}

	@Provides
	AppStore appStore(Config config, IAsyncHttpClient httpClient) {
		return HttpAppStore.create(config.get("appStoreUrl"), httpClient);
	}

	@Provides
	@ContainerScope
	FsClient fsClient(KeyPair keyPair, GlobalFsDriver fsDriver, SimKey simKey, @AppDir String appDir) {
		GlobalFsAdapter adapt = fsDriver.adapt(keyPair);
		adapt.setCurrentSimKey(simKey);
		return adapt.subfolder(appDir);
	}

	@Provides
	@Eager
	OTDriver otDriver(GlobalOTNode node, SimKey simKey) {
		return new OTDriver(node, simKey);
	}

	@Provides
	@Eager
	SimKey simKey(Config config) {
		return config.get(ofSimKey(), "credentials.simKey", DEFAULT_SIM_KEY);
	}

	@Provides
	GlobalKvDriver<String, UserId> kvDriver(GlobalKvNode node) {
		return GlobalKvDriver.create(node, REGISTRY.get(String.class), REGISTRY.get(UserId.class));
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config.getChild("executor"));
	}

	@Provides
	@Eager
	OTSystem<AlbumOperation> system() {
		return AlbumOtSystem.SYSTEM;
	}

	@Provides
	@ContainerScope
	AlbumOtState albumOtState() {
		return new AlbumOtState();
	}

	@Provides
	@ContainerScope
	ObjectDisplayRegistry objectDisplayRegistry(AlbumOtState albumOtState) {
		Map<String, Album> map = albumOtState.getMap();
		return ObjectDisplayRegistry.create()
				.withDisplay(AlbumAddOperation.class,
						($, p) -> (p.isRemove() ? "Remove" : "Add") + " album '" + shortText(p.getTitle()) + '\'',
						(r, p) -> r.getShortDisplay(p) + " with id " + hashId(p.getAlbumId()) + " and description '" + text(p.getDescription()) + "'")
				.withDisplay(AlbumAddPhotoOperation.class,
						($, p) -> {
							Album album = map.get(p.getAlbumId());
							return (p.isRemove() ? "Remove photo from album '" : "Add photo to album '") +
									(album == null ?
											shortHashId(p.getAlbumId()) :
											shortText(album.getTitle()))
									+ '\'';
						},
						($, p) -> {
							Album album = map.get(p.getAlbumId());
							return (p.isRemove() ? "Remove" : "Add") + " photo '" + text(p.getPhoto().getFilename()) +
									"' " + (p.isRemove() ? "from" : "to") + " album '" + (album == null ? hashId(p.getAlbumId()) : text(album.getTitle())) +
									"' " + ts(p.getPhoto().getTimeUpload()) +
									" with description '" + text(p.getPhoto().getDescription()) +
									"' and id " + hashId(p.getPhotoId());
						})
				.withDisplay(AlbumChangeOperation.class,
						($, p) -> "set album " + shortHashId(p.getAlbumId()) + " title to '" + text(p.getNextTitle()) + "', description to '" + text(p.getNextDescription()) + '\'',
						($, p) -> {
							String result = "";
							if (!p.getPrevTitle().equals(p.getNextTitle())) {
								result += "change album " + hashId(p.getAlbumId()) + " title from '" + text(p.getPrevTitle()) + "' to '" + text(p.getNextTitle()) + '\'';
							}
							if (!p.getPrevDescription().equals(p.getNextDescription())) {
								boolean titleChanged = !result.isEmpty();
								if (titleChanged) result += ", ";
								result += "change";
								if (!titleChanged) result += " album " + hashId(p.getAlbumId());
								result += " description from '" + p.getPrevDescription() + "' to '" + p.getNextDescription() + '\'';
							}
							if (result.isEmpty()) {
								result += "nothing has been changed";
							}
							result += " " + ts(p.getMetadata().getTimestamp());
							return result;
						})
				.withDisplay(AlbumChangePhotoOperation.class,
						($, p) -> {
							Album album = map.get(p.getAlbumId());
							Photo photo = album == null ? null : album.getPhoto(p.getPhotoId());
							return (p.getDescription().isEmpty() ?
									("nothing has been changed for photo ") :
									"change description of photo ") +
									(photo == null ?
											shortHashId(p.getPhotoId()) :
											text(photo.getFilename()));
						},
						($, p) -> {
							Album album = map.get(p.getAlbumId());
							Photo photo = album == null ? null : album.getPhoto(p.getPhotoId());
							return (p.getDescription().isEmpty() ?
									("nothing has been changed for photo ") :
									("change description of photo " +
											(photo == null ? hashId(p.getPhotoId()) : text(photo.getFilename()))
											+ " in album " + (album == null ? hashId(p.getAlbumId()) : text(album.getTitle())) +
											" from '" + text(p.getPrevDescription()) + "' to '" + text(p.getNextDescription()) +
											"' " + ts(p.getDescription().getTimestamp())));
						});

	}

}
