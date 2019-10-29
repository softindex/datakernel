package io.global.photos.container;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.*;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.remotefs.FsClient;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.kv.api.KvClient;
import io.global.ot.api.CommitId;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.service.UserContainer;
import io.global.photos.dao.AlbumDao;
import io.global.photos.dao.AlbumDaoImpl;
import io.global.photos.dao.MainDao;
import io.global.photos.dao.MainDaoImpl;
import io.global.photos.ot.AlbumOtState;
import io.global.photos.ot.UserId;
import io.global.photos.ot.operation.AlbumOperation;
import io.global.session.KvSessionStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

import static io.datakernel.async.util.LogUtils.toLogger;
import static io.global.photos.dao.AlbumDao.ROOT_ALBUM;
import static io.global.photos.dao.AlbumDaoImpl.*;
import static io.global.photos.ot.AlbumOtSystem.SYSTEM;
import static io.global.photos.util.Utils.REGISTRY;
import static java.util.Collections.emptySet;

public class GlobalPhotosContainer implements UserContainer {
	private static final Logger logger = LoggerFactory.getLogger(GlobalPhotosContainer.class);
	private final Eventloop eventloop;
	private final KeyPair keys;

	private final OTStateManager<CommitId, AlbumOperation> mainStateManager;
	private final Executor executor;
	private final KvSessionStore<UserId> sessionStore;
	private final MainDao mainDao;

	private GlobalPhotosContainer(Eventloop eventloop, KeyPair keys, KvSessionStore<UserId> sessionStore,
								 OTStateManager<CommitId, AlbumOperation> mainStateManager,
								 FsClient fsClient, Builder albumBuilder, Executor executor) {
		this.keys = keys;
		this.eventloop = eventloop;
		this.sessionStore = sessionStore;
		this.mainStateManager = mainStateManager;
		this.executor = executor;
		this.mainDao = MainDaoImpl.create(this, albumBuilder, fsClient);
	}

	public static GlobalPhotosContainer create(Eventloop eventloop, PrivKey privKey, OTDriver otDriver, KvClient<String, UserId> kvClient,
											  RepoNames names, AlbumDaoImpl.Builder albumBuilder, FsClient fsClient, Executor executor) {
		KeyPair keys = privKey.computeKeys();
		StructuredCodec<AlbumOperation> albumOpCodec = REGISTRY.get(AlbumOperation.class);
		KvSessionStore<UserId> sessionStore = KvSessionStore.create(eventloop, kvClient, names.getSession());
		OTStateManager<CommitId, AlbumOperation> albumsStateManager =
				createStateManager(names.getAlbums(), albumOpCodec, SYSTEM, new AlbumOtState(), eventloop, keys, otDriver);
		return new GlobalPhotosContainer(eventloop, keys, sessionStore, albumsStateManager, fsClient, albumBuilder, executor);
	}

	public static <D> OTStateManager<CommitId, D> createStateManager(String repoName, StructuredCodec<D> diffCodec, OTSystem<D> otSystem, OTState<D> state,
																	 Eventloop eventloop, KeyPair keys, OTDriver otDriver) {
		OTRepositoryAdapter<D> repositoryAdapter = new OTRepositoryAdapter<>(otDriver, MyRepositoryId.of(keys.getPrivKey(), repoName, diffCodec), emptySet());
		OTUplink<CommitId, D, OTCommit<CommitId, D>> node = OTUplinkImpl.create(repositoryAdapter, otSystem);
		return OTStateManager.create(eventloop, otSystem, node, state);
	}

	@Override
	@NotNull
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<?> start() {
		return Promises.all(mainStateManager.start(), sessionStore.start())
				.then($ -> {
					AlbumDao albumDao = mainDao.getAlbumDao(ROOT_ALBUM);
					return albumDao == null ?
							mainDao.crateAlbum(ROOT_ALBUM, ROOT_ALBUM, "") :
							Promise.complete();
				})
				.whenComplete(toLogger(logger, "start"));
	}

	@NotNull
	@Override
	public Promise<?> stop() {
		Promise<Void> threadsStop = mainStateManager.stop();
		return Promises.all(threadsStop, sessionStore.stop())
				.whenComplete(toLogger(logger, "stop"));
	}

	public OTStateManager<CommitId, AlbumOperation> getMainState() {
		return mainStateManager;
	}

	@Override
	public KeyPair getKeys() {
		return keys;
	}

	public MainDao getMainDao() {
		return mainDao;
	}

	public KvSessionStore<UserId> getSessionStore() {
		return sessionStore;
	}

	public Executor getExecutor() {
		return executor;
	}
}
