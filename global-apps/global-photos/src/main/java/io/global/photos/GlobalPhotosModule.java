package io.global.photos;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.common.MemSize;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Eager;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.http.loader.StaticLoader;
import io.datakernel.ot.*;
import io.datakernel.remotefs.FsClient;
import io.global.appstore.AppStore;
import io.global.appstore.HttpAppStore;
import io.global.common.KeyPair;
import io.global.common.SimKey;
import io.global.fs.local.GlobalFsDriver;
import io.global.kv.GlobalKvDriver;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.KvClient;
import io.global.mustache.MustacheTemplater;
import io.global.ot.api.CommitId;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.ContainerServlet;
import io.global.ot.session.UserId;
import io.global.photos.container.GlobalPhotosContainer;
import io.global.photos.container.RepoNames;
import io.global.photos.dao.AlbumDaoImpl;
import io.global.photos.dao.AlbumDaoImpl.ThumbnailDesc;
import io.global.photos.dao.MainDao;
import io.global.photos.dao.MainDaoImpl;
import io.global.photos.http.PublicServlet;
import io.global.photos.ot.AlbumOtState;
import io.global.photos.ot.operation.AlbumOperation;
import io.global.session.KvSessionStore;

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
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.launchers.Initializers.sslServerInitializer;
import static io.global.ot.OTUtils.POLL_RETRY_POLICY;
import static io.global.photos.ot.AlbumOtSystem.SYSTEM;
import static io.global.photos.util.Utils.REGISTRY;
import static io.global.photos.util.Utils.renderErrors;
import static java.util.Collections.emptySet;
import static org.imgscalr.Scalr.Method.SPEED;
import static org.imgscalr.Scalr.Method.ULTRA_QUALITY;

public class GlobalPhotosModule extends AbstractModule {
	public static final SimKey DEFAULT_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final Path DEFAULT_STATIC_PATH = Paths.get("static/files");

	private final String albumFsDir;
	private final RepoNames forumRepoNames;

	@Override
	protected void configure() {
		bind(GlobalPhotosContainer.class).in(ContainerScope.class);
		bind(MainDao.class).to(MainDaoImpl.class).in(ContainerScope.class);
		bind(CodecFactory.class).toInstance(REGISTRY);
		bind(RepoNames.class).toInstance(forumRepoNames);
	}

	public GlobalPhotosModule(String albumFsDir, RepoNames forumRepoNames) {
		this.albumFsDir = albumFsDir;
		this.forumRepoNames = forumRepoNames;
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
	FsClient fsClient(KeyPair keyPair, GlobalFsDriver fsDriver) {
		return fsDriver.adapt(keyPair).subfolder(albumFsDir);
	}

	@Provides
	@ContainerScope
	KvClient<String, UserId> kvClient(KeyPair keyPair, GlobalKvDriver<String, UserId> kvDriver) {
		return kvDriver.adapt(keyPair);
	}

	@Provides
	@Eager
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

	@ContainerScope
	@Provides
	KvSessionStore<UserId> sessionStore(Eventloop eventloop, KvClient<String, UserId> kvClient, RepoNames names) {
		return KvSessionStore.create(eventloop, kvClient, names.getSession());
	}

	@ContainerScope
	@Provides
	OTStateManager<CommitId, AlbumOperation> stateManager(RepoNames names, CodecFactory registry, Eventloop eventloop, KeyPair keys, OTDriver otDriver) {
		StructuredCodec<AlbumOperation> albumOpCodec = registry.get(AlbumOperation.class);
		return createStateManager(names.getAlbums(), albumOpCodec, SYSTEM, new AlbumOtState(), eventloop, keys, otDriver);
	}

	public static <D> OTStateManager<CommitId, D> createStateManager(String repoName, StructuredCodec<D> diffCodec, OTSystem<D> otSystem, OTState<D> state,
																	 Eventloop eventloop, KeyPair keys, OTDriver otDriver) {
		OTRepositoryAdapter<D> repositoryAdapter = new OTRepositoryAdapter<>(otDriver, MyRepositoryId.of(keys.getPrivKey(), repoName, diffCodec), emptySet());
		OTUplink<CommitId, D, OTCommit<CommitId, D>> node = OTUplinkImpl.create(repositoryAdapter, otSystem);
		return OTStateManager.create(eventloop, otSystem, node, state)
				.withPoll(POLL_RETRY_POLICY);
	}
}