package io.global.forum.container;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.*;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.TypeT;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.forum.dao.ForumDao;
import io.global.forum.dao.ForumDaoImpl;
import io.global.forum.dao.ThreadDao;
import io.global.forum.dao.ThreadDaoImpl;
import io.global.forum.ot.ForumMetadata;
import io.global.forum.ot.MapOTStateListenerProxy;
import io.global.forum.ot.post.ThreadOTState;
import io.global.forum.ot.post.ThreadOTSystem;
import io.global.forum.ot.post.operation.ThreadOperation;
import io.global.forum.ot.session.SessionOTState;
import io.global.forum.ot.session.SessionOTSystem;
import io.global.forum.ot.session.UserIdSessionStore;
import io.global.forum.ot.session.operation.SessionOperation;
import io.global.forum.pojo.IpBanState;
import io.global.forum.pojo.ThreadMetadata;
import io.global.forum.pojo.UserData;
import io.global.forum.pojo.UserId;
import io.global.ot.api.CommitId;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.map.MapOTState;
import io.global.ot.map.MapOTSystem;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.ot.service.UserContainer;
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueContainer;
import io.global.ot.value.ChangeValueOTSystem;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static io.datakernel.util.LogUtils.toLogger;
import static io.global.forum.Utils.REGISTRY;
import static java.util.Collections.emptySet;

public final class ForumUserContainer implements UserContainer {
	private static final Logger logger = LoggerFactory.getLogger(ForumUserContainer.class);

	private final Eventloop eventloop;
	private final OTDriver otDriver;
	private final FsClient fsClient;
	private final KeyPair keys;

	private final String threadRepoPrefix;
	private final StructuredCodec<ThreadOperation> postOpCodec;
	private final OTStateManager<CommitId, ChangeValue<ForumMetadata>> metadataStateManager;
	private final OTStateManager<CommitId, MapOperation<UserId, UserData>> usersStateManager;
	private final OTStateManager<CommitId, MapOperation<Long, IpBanState>> bansStateManager;
	private final OTStateManager<CommitId, SessionOperation> sessionStateManager;

	private final OTStateManager<CommitId, MapOperation<Long, ThreadMetadata>> threadsStateManager;

	private final Map<Long, Promise<OTStateManager<CommitId, ThreadOperation>>> pendingThreadStateManagers = new HashMap<>();
	private final Map<Long, OTStateManager<CommitId, ThreadOperation>> threadStateManagers = new HashMap<>();
	private final Map<Long, ThreadDao> threadDaos = new HashMap<>();

	private final UserIdSessionStore sessionStore;
	private final MapOTStateListenerProxy<Long, ThreadMetadata> threadsState;

	private final ForumDao forumDao;

	private ForumUserContainer(
			Eventloop eventloop,
			OTDriver otDriver,
			FsClient fsClient,
			KeyPair keys,
			ForumRepoNames names) {
		this.eventloop = eventloop;
		this.otDriver = otDriver;
		this.fsClient = fsClient;
		this.keys = keys;

		this.threadRepoPrefix = names.getThreadRepoPrefix();
		this.postOpCodec = REGISTRY.get(ThreadOperation.class);

		this.metadataStateManager = createStateManager(names.getMetadata(), REGISTRY.get(new TypeT<ChangeValue<ForumMetadata>>() {}), ChangeValueOTSystem.get(), ChangeValueContainer.of(ForumMetadata.EMPTY));
		this.usersStateManager = createStateManager(names.getUsers(), REGISTRY.get(new TypeT<MapOperation<UserId, UserData>>() {}), MapOTSystem.create(), new MapOTState<>());
		this.bansStateManager = createStateManager(names.getBans(), REGISTRY.get(new TypeT<MapOperation<Long, IpBanState>>() {}), MapOTSystem.create(), new MapOTState<>());
		this.sessionStateManager = createStateManager(names.getSession(), REGISTRY.get(new TypeT<SessionOperation>() {}), SessionOTSystem.SYSTEM, new SessionOTState());

		this.sessionStore = UserIdSessionStore.create(this.sessionStateManager);
		this.threadsState = new MapOTStateListenerProxy<>();
		this.threadsStateManager = createStateManager(names.getThreads(), REGISTRY.get(new TypeT<MapOperation<Long, ThreadMetadata>>() {}), MapOTSystem.create(), threadsState);

		forumDao = new ForumDaoImpl(this);
	}

	public static ForumUserContainer create(
			Eventloop eventloop,
			PrivKey privKey,
			OTDriver otDriver,
			FsClient fsClient,
			ForumRepoNames names) {
		return new ForumUserContainer(eventloop, otDriver, fsClient, privKey.computeKeys(), names);
	}

	@Override
	@NotNull
	public Eventloop getEventloop() {
		return eventloop;
	}

	public ForumDao getForumDao() {
		return forumDao;
	}

	@NotNull
	@Override
	public Promise<?> start() {
		Promise<Void> threadsStart = threadsStateManager.start()
				.then($ -> Promises.all(threadsState
						.getMap()
						.keySet()
						.stream()
						.map(this::ensureThread)))
				.whenResult($ -> threadsState.onOperationReceived(op -> {
					Map<Long, SetValue<ThreadMetadata>> operations = op.getOperations();
					operations.forEach((id, setValue) -> {
						if (setValue.getNext() == null) {
							logger.info("Removing thread {} and post state manager", setValue.getPrev());
							removeThread(id);
						} else if (setValue.getPrev() == null) {
							logger.info("Adding thread {} and post state manager", setValue.getNext());
							ensureThread(id);
						}
					});
				}));
		return Promises.all(metadataStateManager.start(), usersStateManager.start(), bansStateManager.start(), sessionStateManager.start(), threadsStart)
				.then($ -> sessionStore.start())
				.whenComplete(toLogger(logger, "start"));
	}

	@NotNull
	@Override
	public Promise<?> stop() {
		Promise<Void> threadsStop = threadsStateManager.stop()
				.then($ -> Promises.all(Stream.concat(
						threadStateManagers.values().stream().map(OTStateManager::stop),
						pendingThreadStateManagers.values().stream().map(s -> s.then(OTStateManager::stop)))))
				.whenResult($ -> threadStateManagers.clear());

		return Promises.all(metadataStateManager.stop(), usersStateManager.stop(), bansStateManager.stop(), sessionStateManager.stop(), threadsStop)
				.then($ -> sessionStore.stop())
				.whenComplete(toLogger(logger, "stop"));
	}

	public OTStateManager<CommitId, ChangeValue<ForumMetadata>> getMetadataStateManager() {
		return metadataStateManager;
	}

	public OTStateManager<CommitId, MapOperation<UserId, UserData>> getUsersStateManager() {
		return usersStateManager;
	}

	public OTStateManager<CommitId, MapOperation<Long, IpBanState>> getBansStateManager() {
		return bansStateManager;
	}

	public OTStateManager<CommitId, MapOperation<Long, ThreadMetadata>> getThreadsStateManager() {
		return threadsStateManager;
	}

	public FsClient getFsClient() {
		return fsClient;
	}

	public KeyPair getKeys() {
		return keys;
	}

	@Nullable
	public ThreadDao getThreadDao(Long threadId) {
		return threadDaos.get(threadId);
	}

	public UserIdSessionStore getSessionStore() {
		return sessionStore;
	}

	private Promise<OTStateManager<CommitId, ThreadOperation>> ensureThread(Long threadId) {
		return pendingThreadStateManagers.computeIfAbsent(threadId,
				tid -> {
					String repoName = threadRepoPrefix + "/" + Long.toUnsignedString(tid, 16);
					OTStateManager<CommitId, ThreadOperation> stateManager = createStateManager(repoName, postOpCodec, ThreadOTSystem.SYSTEM, new ThreadOTState());
					return stateManager.start()
							.whenResult($ -> {
								threadStateManagers.put(tid, stateManager);
								threadDaos.put(tid, new ThreadDaoImpl(forumDao, tid, stateManager, fsClient));
							})
							.whenComplete(() -> pendingThreadStateManagers.remove(tid))
							.map($2 -> stateManager);
				})
				.whenComplete(toLogger(logger, "ensureThread", threadId));
	}

	private Promise<Void> removeThread(Long threadId) {
		threadDaos.remove(threadId);
		OTStateManager<CommitId, ThreadOperation> stateManager = threadStateManagers.remove(threadId);
		return (stateManager != null ? stateManager.stop() : Promise.complete())
				.whenComplete(toLogger(logger, "removeThread", threadId));
	}

	private <D> OTStateManager<CommitId, D> createStateManager(String repoName, StructuredCodec<D> diffCodec, OTSystem<D> otSystem, OTState<D> state) {
		OTRepositoryAdapter<D> repositoryAdapter = new OTRepositoryAdapter<>(otDriver, MyRepositoryId.of(keys.getPrivKey(), repoName, diffCodec), emptySet());
		OTNodeImpl<CommitId, D, OTCommit<CommitId, D>> node = OTNodeImpl.create(repositoryAdapter, otSystem);
		return OTStateManager.create(eventloop, otSystem, node, state);
	}
}
