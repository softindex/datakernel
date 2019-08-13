package io.global.video.container;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.ot.*;
import io.datakernel.remotefs.FsClient;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.fs.local.GlobalFsDriver;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.map.MapOTState;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.video.dao.CommentDao;
import io.global.video.dao.CommentDaoImpl;
import io.global.video.dao.VideoDao;
import io.global.video.dao.VideoDaoImpl;
import io.global.video.ot.VideosState;
import io.global.video.pojo.Comment;
import io.global.video.pojo.VideoMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.global.video.Utils.COMMENTS_OT_SYSTEM;
import static io.global.video.Utils.VIDEOS_OT_SYSTEM;
import static java.util.Collections.emptySet;

public final class VideoUserContainer implements EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(VideoUserContainer.class);

	private final Eventloop eventloop;
	private final OTDriver otDriver;
	private final FsClient fsClient;
	private final KeyPair keys;
	private final String commentsRepoPrefix;
	private final StructuredCodec<MapOperation<Long, Comment>> commentOpCodec;
	private final OTStateManager<CommitId, MapOperation<String, VideoMetadata>> videoStateManager;
	private final Map<String, Promise<OTStateManager<CommitId, MapOperation<Long, Comment>>>> commentsStateManagers = new HashMap<>();

	private VideoUserContainer(
			Eventloop eventloop,
			OTDriver otDriver,
			FsClient fsClient,
			KeyPair keys,
			String commentsRepoPrefix,
			StructuredCodec<MapOperation<Long, Comment>> commentOpCodec,
			OTStateManager<CommitId, MapOperation<String, VideoMetadata>> videoStateManager
	) {
		this.eventloop = eventloop;
		this.otDriver = otDriver;
		this.fsClient = fsClient;
		this.keys = keys;
		this.commentsRepoPrefix = commentsRepoPrefix;
		this.commentOpCodec = commentOpCodec;
		this.videoStateManager = videoStateManager;
	}

	public static VideoUserContainer create(
			Eventloop eventloop,
			PrivKey privKey,
			OTDriver otDriver,
			GlobalFsDriver fsDriver,
			String videosFolder,
			String videosRepoName,
			String commentsRepoPrefix,
			StructuredCodec<MapOperation<String, VideoMetadata>> videoOpCodec,
			StructuredCodec<MapOperation<Long, Comment>> commentOpCodec
	) {
		MyRepositoryId<MapOperation<String, VideoMetadata>> videosMyRepositoryId =
				new MyRepositoryId<>(RepoID.of(privKey, videosRepoName), privKey, videoOpCodec);
		OTStateManager<CommitId, MapOperation<String, VideoMetadata>> videosStateManager =
				createStateManager(eventloop, otDriver, videosMyRepositoryId, VIDEOS_OT_SYSTEM, new VideosState());
		FsClient fsClient = fsDriver.adapt(privKey).subfolder(videosFolder);
		return new VideoUserContainer(eventloop, otDriver, fsClient, privKey.computeKeys(), commentsRepoPrefix, commentOpCodec, videosStateManager);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<?> start() {
		VideosState state = (VideosState) videoStateManager.getState();
		return videoStateManager.start()
				.then($ -> Promises.all(state
						.getVideos()
						.keySet()
						.stream()
						.map(this::ensureCommentStateManager)))
				.whenResult($ -> state.setListener(op -> {
					Map<String, SetValue<VideoMetadata>> operations = op.getOperations();
					operations.forEach((id, setValue) -> {
						if (setValue.getNext() == null) {
							logger.info("Removing video {} and comment state manager", setValue.getPrev());
							removeCommentStateManager(id);
						} else if (setValue.getPrev() == null) {
							logger.info("Adding video {} and comment state manager", setValue.getNext());
							ensureCommentStateManager(id);
						}
					});
				}))
				.whenComplete(toLogger(logger, thisMethod()));
	}

	@NotNull
	@Override
	public Promise<?> stop() {
		return videoStateManager.stop()
				.then($ -> Promises.all(new HashSet<>(commentsStateManagers.keySet()).stream()
						.map(this::removeCommentStateManager)))
				.whenComplete(toLogger(logger, thisMethod()));
	}

	@NotNull
	public VideoDao getVideoDao() {
		return new VideoDaoImpl(fsClient, videoStateManager);
	}

	public Promise<@Nullable CommentDao> getCommentDao(String videoId) {
		if (!commentsStateManagers.containsKey(videoId)) {
			return Promise.of(null);
		}
		return commentsStateManagers.get(videoId)
				.map(CommentDaoImpl::new);
	}

	public KeyPair getKeys() {
		return keys;
	}

	private Promise<OTStateManager<CommitId, MapOperation<Long, Comment>>> ensureCommentStateManager(String videoId) {
		return Promise.complete()
				.then($1 -> {
					if (!commentsStateManagers.containsKey(videoId)) {
						RepoID repoID = RepoID.of(keys, commentsRepoPrefix + "/" + videoId);
						MyRepositoryId<MapOperation<Long, Comment>> myRepositoryId = new MyRepositoryId<>(repoID, keys.getPrivKey(), commentOpCodec);
						MapOTState<Long, Comment> state = new MapOTState<>(new LinkedHashMap<>());
						OTStateManager<CommitId, MapOperation<Long, Comment>> stateManager =
								createStateManager(eventloop, otDriver, myRepositoryId, COMMENTS_OT_SYSTEM, state);
						Promise<OTStateManager<CommitId, MapOperation<Long, Comment>>> stateManagerPromise = stateManager.start()
								.map($2 -> stateManager);
						commentsStateManagers.put(videoId, stateManagerPromise);
						stateManagerPromise
								.whenException($ -> commentsStateManagers.remove(videoId));
						return stateManagerPromise;
					} else {
						return commentsStateManagers.get(videoId);
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), videoId));
	}

	private Promise<Void> removeCommentStateManager(String videoId) {
		Promise<OTStateManager<CommitId, MapOperation<Long, Comment>>> stateManager = commentsStateManagers.remove(videoId);
		return Promise.complete()
				.then($ -> stateManager != null ?
						stateManager
								.then(OTStateManager::stop) :
						Promise.complete())
				.whenComplete(toLogger(logger, thisMethod(), videoId));
	}

	private static <D> OTStateManager<CommitId, D> createStateManager(Eventloop eventloop, OTDriver otDriver,
			MyRepositoryId<D> myRepositoryId, OTSystem<D> otSystem, OTState<D> state) {
		OTRepositoryAdapter<D> repositoryAdapter = new OTRepositoryAdapter<>(otDriver, myRepositoryId, emptySet());
		OTNodeImpl<CommitId, D, OTCommit<CommitId, D>> node = OTNodeImpl.create(repositoryAdapter, otSystem);
		return OTStateManager.create(eventloop, otSystem, node, state);
	}

}
