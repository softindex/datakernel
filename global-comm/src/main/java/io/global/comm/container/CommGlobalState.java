package io.global.comm.container;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.*;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.remotefs.FsClient;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.CommDaoImpl;
import io.global.comm.dao.ThreadDao;
import io.global.comm.dao.ThreadDaoImpl;
import io.global.comm.ot.MapOTStateListenerProxy;
import io.global.comm.ot.post.ThreadOTState;
import io.global.comm.ot.post.ThreadOTSystem;
import io.global.comm.ot.post.operation.ThreadOperation;
import io.global.comm.ot.session.KvSessionStore;
import io.global.comm.pojo.IpBanState;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserData;
import io.global.comm.pojo.UserId;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.kv.api.KvClient;
import io.global.ot.api.CommitId;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.map.MapOTState;
import io.global.ot.map.MapOTSystem;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static io.datakernel.async.util.LogUtils.toLogger;
import static io.global.comm.util.Utils.REGISTRY;
import static java.util.Collections.emptySet;

public final class CommGlobalState {
	private static final Logger logger = LoggerFactory.getLogger(CommGlobalState.class);

	private final Eventloop eventloop;
	private final OTDriver otDriver;
	private final FsClient fsClient;
	private final KeyPair keys;

	private final String threadRepoPrefix;
	private final StructuredCodec<ThreadOperation> postOpCodec;
	private final OTStateManager<CommitId, MapOperation<UserId, UserData>> usersStateManager;
	private final OTStateManager<CommitId, MapOperation<String, IpBanState>> bansStateManager;

	private final OTStateManager<CommitId, MapOperation<String, ThreadMetadata>> threadsStateManager;

	private final Map<String, Promise<OTStateManager<CommitId, ThreadOperation>>> pendingThreadStateManagers = new HashMap<>();
	private final Map<String, OTStateManager<CommitId, ThreadOperation>> threadStateManagers = new HashMap<>();
	private final Map<String, ThreadDao> threadDaos = new HashMap<>();

	private final KvSessionStore<UserId> sessionStore;
	private final MapOTStateListenerProxy<String, ThreadMetadata> threadsState;

	private final CommDao commDao;

	private CommGlobalState(
			Eventloop eventloop,
			OTDriver otDriver,
			KvClient<String, UserId> kvClient,
			FsClient fsClient,
			KeyPair keys,
			CommRepoNames names) {
		this.eventloop = eventloop;
		this.otDriver = otDriver;
		this.fsClient = fsClient;
		this.keys = keys;

		this.threadRepoPrefix = names.getThreadRepoPrefix();
		this.postOpCodec = REGISTRY.get(ThreadOperation.class);

		this.usersStateManager = createStateManager(names.getUsers(), REGISTRY.get(new TypeT<MapOperation<UserId, UserData>>() {}), MapOTSystem.create(), new MapOTState<>());
		this.bansStateManager = createStateManager(names.getBans(), REGISTRY.get(new TypeT<MapOperation<String, IpBanState>>() {}), MapOTSystem.create(), new MapOTState<>());

		this.sessionStore = KvSessionStore.create(eventloop, kvClient, names.getSession());
		this.threadsState = new MapOTStateListenerProxy<>();
		this.threadsStateManager = createStateManager(names.getThreads(), REGISTRY.get(new TypeT<MapOperation<String, ThreadMetadata>>() {}), MapOTSystem.create(), threadsState);

		commDao = new CommDaoImpl(this);
	}

	public static CommGlobalState create(
			Eventloop eventloop,
			PrivKey privKey,
			OTDriver otDriver,
			KvClient<String, UserId> kvClient,
			FsClient fsClient,
			CommRepoNames names) {
		return new CommGlobalState(eventloop, otDriver, kvClient, fsClient, privKey.computeKeys(), names);
	}

	public CommDao getCommDao() {
		return commDao;
	}

	public Promise<?> start() {
		Promise<Void> threadsStart = threadsStateManager.start()
				.then($ -> Promises.all(threadsState
						.getMap()
						.keySet()
						.stream()
						.map(this::ensureThread)))
				.whenResult($ -> threadsState.onOperationReceived(op -> {
					Map<String, SetValue<ThreadMetadata>> operations = op.getOperations();
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
		return Promises.all(usersStateManager.start(), bansStateManager.start(), threadsStart, sessionStore.start())
				.whenComplete(toLogger(logger, "start"));
	}

	public Promise<?> stop() {
		Promise<Void> threadsStop = threadsStateManager.stop()
				.then($ -> Promises.all(Stream.concat(
						threadStateManagers.values().stream().map(OTStateManager::stop),
						pendingThreadStateManagers.values().stream().map(s -> s.then(OTStateManager::stop)))))
				.whenResult($ -> threadStateManagers.clear());

		return Promises.all(usersStateManager.stop(), bansStateManager.stop(), threadsStop, sessionStore.stop())
				.whenComplete(toLogger(logger, "stop"));
	}

	public OTStateManager<CommitId, MapOperation<UserId, UserData>> getUsersStateManager() {
		return usersStateManager;
	}

	public OTStateManager<CommitId, MapOperation<String, IpBanState>> getBansStateManager() {
		return bansStateManager;
	}

	public OTStateManager<CommitId, MapOperation<String, ThreadMetadata>> getThreadsStateManager() {
		return threadsStateManager;
	}

	public FsClient getFsClient() {
		return fsClient;
	}

	public KeyPair getKeys() {
		return keys;
	}

	@Nullable
	public ThreadDao getThreadDao(String threadId) {
		return threadDaos.get(threadId);
	}

	public SessionStore<UserId> getSessionStore() {
		return sessionStore;
	}

	private Promise<OTStateManager<CommitId, ThreadOperation>> ensureThread(String threadId) {
		return pendingThreadStateManagers.computeIfAbsent(threadId,
				tid -> {
					String repoName = threadRepoPrefix + "/" + tid;
					OTStateManager<CommitId, ThreadOperation> stateManager = createStateManager(repoName, postOpCodec, ThreadOTSystem.SYSTEM, new ThreadOTState());
					return stateManager.start()
							.whenResult($ -> {
								threadStateManagers.put(tid, stateManager);
								threadDaos.put(tid, new ThreadDaoImpl(commDao, tid, stateManager, fsClient.subfolder(tid)));
							})
							.whenComplete(() -> pendingThreadStateManagers.remove(tid))
							.map($2 -> stateManager);
				})
				.whenComplete(toLogger(logger, "ensureThread", threadId));
	}

	private Promise<Void> removeThread(String threadId) {
		threadDaos.remove(threadId);
		OTStateManager<CommitId, ThreadOperation> stateManager = threadStateManagers.remove(threadId);
		return (stateManager != null ? stateManager.stop() : Promise.complete())
				.whenComplete(toLogger(logger, "removeThread", threadId));
	}

	public <D> OTStateManager<CommitId, D> createStateManager(String repoName, StructuredCodec<D> diffCodec, OTSystem<D> otSystem, OTState<D> state) {
		OTRepositoryAdapter<D> repositoryAdapter = new OTRepositoryAdapter<>(otDriver, MyRepositoryId.of(keys.getPrivKey(), repoName, diffCodec), emptySet());
		OTUplink<CommitId, D, OTCommit<CommitId, D>> uplink = OTUplinkImpl.create(repositoryAdapter, otSystem);
		return OTStateManager.create(eventloop, otSystem, uplink, state);
	}
}
