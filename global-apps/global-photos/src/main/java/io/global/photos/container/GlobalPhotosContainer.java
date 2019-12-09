package io.global.photos.container;

import io.datakernel.di.annotation.Inject;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.global.ot.StateManagerWithMerger;
import io.global.ot.api.CommitId;
import io.global.ot.service.AbstractUserContainer;
import io.global.photos.dao.AlbumDao;
import io.global.photos.dao.MainDao;
import io.global.photos.dao.MainDaoImpl;
import io.global.photos.ot.operation.AlbumOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

import static io.datakernel.async.util.LogUtils.toLogger;
import static io.global.photos.dao.AlbumDao.ROOT_ALBUM;

public final class GlobalPhotosContainer extends AbstractUserContainer {
	private static final Logger logger = LoggerFactory.getLogger(GlobalPhotosContainer.class);

	@Inject private StateManagerWithMerger<AlbumOperation> mainStateManagerWithMerger;
	@Inject private Executor executor;
	private MainDao mainDao;

	@Inject
	public GlobalPhotosContainer(MainDaoImpl mainDao) {
		mainDao.setContainer(this);
		this.mainDao = mainDao;
	}

	@Override
	protected Promise<?> doStart() {
		return mainStateManagerWithMerger.start()
				.then($ -> {
					AlbumDao albumDao = mainDao.getAlbumDao(ROOT_ALBUM);
					return albumDao == null ?
							mainDao.crateAlbum(ROOT_ALBUM, ROOT_ALBUM, "") :
							Promise.complete();
				})
				.whenComplete(toLogger(logger, "doStart"));
	}

	@Override
	protected Promise<?> doStop() {
		return mainStateManagerWithMerger.stop()
				.whenComplete(toLogger(logger, "doStop"));

	}

	public OTStateManager<CommitId, AlbumOperation> getMainState() {
		return mainStateManagerWithMerger.getStateManager();
	}

	public MainDao getMainDao() {
		return mainDao;
	}

	public Executor getExecutor() {
		return executor;
	}
}
