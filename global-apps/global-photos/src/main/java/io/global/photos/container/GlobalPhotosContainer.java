package io.global.photos.container;

import io.datakernel.di.annotation.Inject;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.common.KeyPair;
import io.global.ot.api.CommitId;
import io.global.ot.service.UserContainer;
import io.global.ot.session.UserId;
import io.global.photos.dao.AlbumDao;
import io.global.photos.dao.MainDao;
import io.global.photos.dao.MainDaoImpl;
import io.global.photos.ot.operation.AlbumOperation;
import io.global.session.KvSessionStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

import static io.datakernel.async.util.LogUtils.toLogger;
import static io.global.photos.dao.AlbumDao.ROOT_ALBUM;

public class GlobalPhotosContainer implements UserContainer {
	private static final Logger logger = LoggerFactory.getLogger(GlobalPhotosContainer.class);
	@Inject private Eventloop eventloop;
	@Inject private KeyPair keys;

	@Inject private OTStateManager<CommitId, AlbumOperation> mainStateManager;
	@Inject private KvSessionStore<UserId> sessionStore;
	@Inject private Executor executor;
	private MainDao mainDao;

	@Inject
	public GlobalPhotosContainer(MainDaoImpl mainDao) {
		mainDao.setContainer(this);
		this.mainDao = mainDao;
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

	@Override
	public SessionStore<UserId> getSessionStore() {
		return sessionStore;
	}

	public MainDao getMainDao() {
		return mainDao;
	}


	public Executor getExecutor() {
		return executor;
	}
}
