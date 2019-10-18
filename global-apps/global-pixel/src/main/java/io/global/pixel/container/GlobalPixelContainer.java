package io.global.pixel.container;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.*;
import io.datakernel.remotefs.FsClient;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.kv.api.KvClient;
import io.global.ot.api.CommitId;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.service.UserContainer;
import io.global.pixel.dao.AlbumDao;
import io.global.pixel.dao.AlbumDaoImpl;
import io.global.pixel.dao.MainDao;
import io.global.pixel.dao.MainDaoImpl;
import io.global.pixel.ot.AlbumOtState;
import io.global.pixel.ot.UserId;
import io.global.pixel.ot.operation.AlbumOperation;
import io.global.pixel.ot.session.KvSessionStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.util.LogUtils.toLogger;
import static io.global.pixel.dao.AlbumDao.ROOT_ALBUM;
import static io.global.pixel.dao.AlbumDaoImpl.*;
import static io.global.pixel.ot.AlbumOtSystem.SYSTEM;
import static io.global.pixel.util.Utils.REGISTRY;
import static java.util.Collections.emptySet;

public class GlobalPixelContainer implements UserContainer {
	private static final Logger logger = LoggerFactory.getLogger(GlobalPixelContainer.class);
	private final Eventloop eventloop;
	private final KeyPair keys;

	private final OTStateManager<CommitId, AlbumOperation> mainStateManager;
	private final KvSessionStore<UserId> sessionStore;
	private final MainDao mainDao;

	private GlobalPixelContainer(Eventloop eventloop, KeyPair keys, KvSessionStore<UserId> sessionStore,
								 OTStateManager<CommitId, AlbumOperation> mainStateManager,
								 FsClient fsClient, Builder albumBuilder) {
		this.keys = keys;
		this.eventloop = eventloop;
		this.sessionStore = sessionStore;
		this.mainStateManager = mainStateManager;
		this.mainDao = new MainDaoImpl(this, albumBuilder, fsClient);
	}

	public static GlobalPixelContainer create(Eventloop eventloop, PrivKey privKey, OTDriver otDriver, KvClient<String, UserId> kvClient,
											  RepoNames names, AlbumDaoImpl.Builder albumBuilder, FsClient fsClient) {
		KeyPair keys = privKey.computeKeys();
		StructuredCodec<AlbumOperation> albumOpCodec = REGISTRY.get(AlbumOperation.class);
		KvSessionStore<UserId> sessionStore = KvSessionStore.create(eventloop, kvClient, names.getSession());
		OTStateManager<CommitId, AlbumOperation> albumsStateManager =
				createStateManager(names.getAlbums(), albumOpCodec, SYSTEM, new AlbumOtState(), eventloop, keys, otDriver);
		return new GlobalPixelContainer(eventloop, keys, sessionStore, albumsStateManager, fsClient, albumBuilder);
	}

	public static <D> OTStateManager<CommitId, D> createStateManager(String repoName, StructuredCodec<D> diffCodec, OTSystem<D> otSystem, OTState<D> state,
																	 Eventloop eventloop, KeyPair keys, OTDriver otDriver) {
		OTRepositoryAdapter<D> repositoryAdapter = new OTRepositoryAdapter<>(otDriver, MyRepositoryId.of(keys.getPrivKey(), repoName, diffCodec), emptySet());
		OTNodeImpl<CommitId, D, OTCommit<CommitId, D>> node = OTNodeImpl.create(repositoryAdapter, otSystem);
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
}
