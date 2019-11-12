package io.global.comm.container;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.InstanceProvider;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.SettablePromise;
import io.datakernel.remotefs.FsClient;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.dao.ThreadDaoImpl;
import io.global.comm.ot.MapOTStateListenerProxy;
import io.global.comm.ot.post.operation.ThreadOperation;
import io.global.comm.pojo.ThreadMetadata;
import io.global.ot.api.CommitId;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.datakernel.async.util.LogUtils.toLogger;

public final class CategoryState {
	private static final Logger logger = LoggerFactory.getLogger(CategoryState.class);

	@Inject
	private Function<String, OTStateManager<CommitId, ThreadOperation>> threadStateManagerFactory;

	@Inject
	private FsClient fsClient;
	@Inject
	private InstanceProvider<CommDao> commDao;

	private final OTStateManager<CommitId, MapOperation<String, ThreadMetadata>> threadsStateManager;
	private final MapOTStateListenerProxy<String, ThreadMetadata> threadsState;

	private final Map<String, Promise<OTStateManager<CommitId, ThreadOperation>>> postStateManagers = new HashMap<>();
	private final Map<String, Promise<ThreadDao>> threadDaos = new HashMap<>();

	@Inject
	public CategoryState(OTStateManager<CommitId, MapOperation<String, ThreadMetadata>> threadsStateManager) {
		this.threadsStateManager = threadsStateManager;
		this.threadsState = (MapOTStateListenerProxy<String, ThreadMetadata>) threadsStateManager.getState();
	}

	public Promise<?> start() {
		return threadsStateManager.start()
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
				}))
				.whenComplete(toLogger(logger, "start"));
	}

	public Promise<?> stop() {
		return threadsStateManager.stop()
				.then($ -> Promises.all(postStateManagers.values().stream().map(s -> s.then(OTStateManager::stop))))
				.whenResult($ -> postStateManagers.clear())
				.whenComplete(toLogger(logger, "stop"));
	}

	private Promise<OTStateManager<CommitId, ThreadOperation>> ensureThread(String threadId) {
		return postStateManagers.computeIfAbsent(threadId,
				tid -> {
					OTStateManager<CommitId, ThreadOperation> stateManager = threadStateManagerFactory.apply(tid);
					SettablePromise<ThreadDao> daoPromise = new SettablePromise<>();
					threadDaos.put(tid, daoPromise);
					return stateManager.start()
							.whenResult($ -> daoPromise.set(new ThreadDaoImpl(commDao.get(), tid, stateManager, fsClient.subfolder(tid))))
							.map($2 -> stateManager);
				})
				.whenComplete(toLogger(logger, "ensureThread", threadId));
	}

	private Promise<Void> removeThread(String threadId) {
		threadDaos.remove(threadId);
		Promise<OTStateManager<CommitId, ThreadOperation>> stateManagerPromise = postStateManagers.remove(threadId);

		if (stateManagerPromise != null) {
			OTStateManager<CommitId, ThreadOperation> stateManager = stateManagerPromise.getResult();
			if (stateManager != null) {
				return stateManager.stop()
						.whenComplete(toLogger(logger, "removeThread", threadId));
			}
		}
		return Promise.complete()
				.whenComplete(toLogger(logger, "removeThread", threadId));
	}

	public OTStateManager<CommitId, MapOperation<String, ThreadMetadata>> getThreadsStateManager() {
		return threadsStateManager;
	}

	public MapOTStateListenerProxy<String, ThreadMetadata> getThreadsState() {
		return threadsState;
	}

	public Map<String, Promise<OTStateManager<CommitId, ThreadOperation>>> getPostStateManagers() {
		return postStateManagers;
	}

	public Map<String, Promise<ThreadDao>> getThreadDaos() {
		return threadDaos;
	}
}
