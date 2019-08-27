package io.global.forum.container;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.*;
import io.datakernel.remotefs.FsClient;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.forum.dao.ForumDao;
import io.global.forum.dao.ForumDaoImpl;
import io.global.forum.ot.ForumMetadata;
import io.global.forum.ot.MapOTStateListenerProxy;
import io.global.forum.ot.post.PostOTSystem;
import io.global.forum.ot.post.ThreadOTState;
import io.global.forum.ot.post.operation.PostOperation;
import io.global.forum.pojo.IpBanState;
import io.global.forum.pojo.ThreadMetadata;
import io.global.forum.pojo.UserData;
import io.global.forum.pojo.UserId;
import io.global.fs.local.GlobalFsDriver;
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

import static io.datakernel.util.LogUtils.toLogger;
import static java.util.Collections.emptySet;

public final class ForumUserContainer implements UserContainer {
	private static final Logger logger = LoggerFactory.getLogger(ForumUserContainer.class);

	private final Eventloop eventloop;
	private final OTDriver otDriver;
	private final FsClient fsClient;
	private final KeyPair keys;

	private final String threadRepoPrefix;
	private final StructuredCodec<PostOperation> postOpCodec;
	private final OTStateManager<CommitId, ChangeValue<ForumMetadata>> metadataStateManager;
	private final OTStateManager<CommitId, MapOperation<UserId, UserData>> usersStateManager;
	private final OTStateManager<CommitId, MapOperation<Long, IpBanState>> bansStateManager;

	private final OTStateManager<CommitId, MapOperation<Long, ThreadMetadata>> threadsStateManager;

	private final Map<Long, Promise<OTStateManager<CommitId, PostOperation>>> threadStateManagers = new HashMap<>();
	private final MapOTStateListenerProxy<Long, ThreadMetadata> threadsState;

	private final ForumDao forumDao;

	private ForumUserContainer(
			Eventloop eventloop,
			OTDriver otDriver,
			FsClient fsClient,
			KeyPair keys,
			ForumRepoNames names,
			StructuredCodec<PostOperation> postOpCodec,
			StructuredCodec<ChangeValue<ForumMetadata>> metaOpCodec,
			StructuredCodec<MapOperation<UserId, UserData>> userOpCodec,
			StructuredCodec<MapOperation<Long, IpBanState>> banOpCodec,
			StructuredCodec<MapOperation<Long, ThreadMetadata>> threadOpCodec) {
		this.eventloop = eventloop;
		this.otDriver = otDriver;
		this.fsClient = fsClient;
		this.keys = keys;

		this.threadRepoPrefix = names.getThreadRepoPrefix();
		this.postOpCodec = postOpCodec;

		this.metadataStateManager = createStateManager(names.getMetadata(), metaOpCodec, ChangeValueOTSystem.get(), ChangeValueContainer.empty());
		this.usersStateManager = createStateManager(names.getUsers(), userOpCodec, MapOTSystem.create(), new MapOTState<>());
		this.bansStateManager = createStateManager(names.getBans(), banOpCodec, MapOTSystem.create(), new MapOTState<>());

		threadsState = new MapOTStateListenerProxy<>();
		this.threadsStateManager = createStateManager(names.getThreads(), threadOpCodec, MapOTSystem.create(), threadsState);

		forumDao = new ForumDaoImpl(this);
	}

	public static ForumUserContainer create(
			Eventloop eventloop,
			PrivKey privKey,
			OTDriver otDriver,
			GlobalFsDriver fsDriver,
			ForumRepoNames names,
			StructuredCodec<ChangeValue<ForumMetadata>> forumMetadataOpCodec,
			StructuredCodec<MapOperation<UserId, UserData>> userOpCodec,
			StructuredCodec<MapOperation<Long, IpBanState>> ipBanOpCodec,
			StructuredCodec<MapOperation<Long, ThreadMetadata>> threadOpCodec,
			StructuredCodec<PostOperation> postOpCodec) {
		return new ForumUserContainer(eventloop, otDriver, fsDriver.adapt(privKey), privKey.computeKeys(), names, postOpCodec, forumMetadataOpCodec, userOpCodec, ipBanOpCodec, threadOpCodec);
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
						.map(this::ensurePostStateManager)))
				.whenResult($ -> threadsState.onOperationReceived(op -> {
					Map<Long, SetValue<ThreadMetadata>> operations = op.getOperations();
					operations.forEach((id, setValue) -> {
						if (setValue.getNext() == null) {
							logger.info("Removing thread {} and post state manager", setValue.getPrev());
							removePostStateManager(id);
						} else if (setValue.getPrev() == null) {
							logger.info("Adding thread {} and post state manager", setValue.getNext());
							ensurePostStateManager(id);
						}
					});
				}));
		return Promises.all(metadataStateManager.start(), usersStateManager.start(), bansStateManager.start(), threadsStart)
				.whenComplete(toLogger(logger, "start"));
	}

	@NotNull
	@Override
	public Promise<?> stop() {
		Promise<Void> threadsStop = threadsStateManager.stop()
				.then($ -> Promises.all(threadStateManagers.values().stream().map(m -> m.then(OTStateManager::stop))))
				.whenResult($ -> threadStateManagers.clear());

		return Promises.all(metadataStateManager.stop(), usersStateManager.stop(), bansStateManager.stop(), threadsStop)
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

	public Promise<@Nullable OTStateManager<CommitId, PostOperation>> getPostStateManager(Long threadId) {
		Promise<OTStateManager<CommitId, PostOperation>> stateManagerPromise = threadStateManagers.get(threadId);
		return stateManagerPromise != null ? stateManagerPromise : Promise.of(null);
	}

	private Promise<OTStateManager<CommitId, PostOperation>> ensurePostStateManager(Long threadId) {
		return threadStateManagers.computeIfAbsent(threadId,
				tid -> {
					String repoName = threadRepoPrefix + "/" + Long.toUnsignedString(tid, 16);
					OTStateManager<CommitId, PostOperation> stateManager = createStateManager(repoName, postOpCodec, PostOTSystem.SYSTEM, new ThreadOTState());
					return stateManager.start()
							.map($2 -> stateManager)
							.whenException($2 -> threadStateManagers.remove(tid));
				})
				.whenComplete(toLogger(logger, "ensurePostStateManager", threadId));
	}

	private Promise<Void> removePostStateManager(Long threadId) {
		Promise<OTStateManager<CommitId, PostOperation>> stateManager = threadStateManagers.remove(threadId);
		return (stateManager != null ? stateManager.then(OTStateManager::stop) : Promise.complete())
				.whenComplete(toLogger(logger, "removePostStateManager", threadId));
	}

	private <D> OTStateManager<CommitId, D> createStateManager(String repoName, StructuredCodec<D> diffCodec, OTSystem<D> otSystem, OTState<D> state) {
		OTRepositoryAdapter<D> repositoryAdapter = new OTRepositoryAdapter<>(otDriver, MyRepositoryId.of(keys.getPrivKey(), repoName, diffCodec), emptySet());
		OTNodeImpl<CommitId, D, OTCommit<CommitId, D>> node = OTNodeImpl.create(repositoryAdapter, otSystem);
		return OTStateManager.create(eventloop, otSystem, node, state);
	}
}
