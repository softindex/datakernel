package io.global.comm.container;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.InstanceProvider;
import io.datakernel.di.core.Key;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.SettablePromise;
import io.datakernel.remotefs.FsClient;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.dao.ThreadDaoImpl;
import io.global.comm.ot.post.operation.ThreadOperation;
import io.global.comm.pojo.ThreadMetadata;
import io.global.common.KeyPair;
import io.global.ot.StateManagerWithMerger;
import io.global.ot.TypedRepoNames;
import io.global.ot.api.RepoID;
import io.global.ot.map.MapOTStateListenerProxy;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.ot.server.GlobalOTNodeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.datakernel.async.util.LogUtils.toLogger;

public final class CategoryState {
	private static final Logger logger = LoggerFactory.getLogger(CategoryState.class);

	@Inject
	private Function<String, StateManagerWithMerger<ThreadOperation>> threadStateManagerWithMergerFactory;

	@Inject
	private FsClient fsClient;
	@Inject
	private InstanceProvider<CommDao> commDao;
	@Inject
	private GlobalOTNodeImpl otNode;
	@Inject
	private TypedRepoNames names;
	@Inject
	private KeyPair keys;

	private final StateManagerWithMerger<MapOperation<String, ThreadMetadata>> threadsStateManagerWithMerger;
	private final MapOTStateListenerProxy<String, ThreadMetadata> threadsState;

	private final Map<String, Promise<StateManagerWithMerger<ThreadOperation>>> postStateManagersWithMergers = new HashMap<>();
	private final Map<String, Promise<ThreadDao>> threadDaos = new HashMap<>();

	@Inject
	public CategoryState(StateManagerWithMerger<MapOperation<String, ThreadMetadata>> threadsStateManagerWithMerger) {
		this.threadsStateManagerWithMerger = threadsStateManagerWithMerger;
		this.threadsState = (MapOTStateListenerProxy<String, ThreadMetadata>) threadsStateManagerWithMerger.getStateManager().getState();
	}

	public Promise<?> start() {
		return otNode.fetch(RepoID.of(keys, names.getRepoName(new Key<MapOperation<String, ThreadMetadata>>() {})))
				.thenEx(($, e) -> threadsStateManagerWithMerger.start())
				.then($ -> Promises.all(threadsState
						.getMap()
						.keySet()
						.stream()
						.map(threadId -> otNode.fetch(RepoID.of(keys, names.getRepoPrefix(Key.of(ThreadOperation.class)) + threadId))
								.thenEx(($2, e) -> ensureThread(threadId)))))
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
		return threadsStateManagerWithMerger.stop()
				.then($ -> Promises.all(postStateManagersWithMergers.values().stream().map(s -> s.then(StateManagerWithMerger::stop))))
				.whenResult($ -> postStateManagersWithMergers.clear())
				.whenComplete(toLogger(logger, "stop"));
	}

	private Promise<StateManagerWithMerger<ThreadOperation>> ensureThread(String threadId) {
		return postStateManagersWithMergers.computeIfAbsent(threadId,
				tid -> {
					StateManagerWithMerger<ThreadOperation> stateManagerWithMerger = threadStateManagerWithMergerFactory.apply(tid);
					SettablePromise<ThreadDao> daoPromise = new SettablePromise<>();
					threadDaos.put(tid, daoPromise);
					return stateManagerWithMerger.start()
							.whenResult($ -> daoPromise.set(new ThreadDaoImpl(commDao.get(), tid, stateManagerWithMerger.getStateManager(), fsClient.subfolder(tid))))
							.map($2 -> stateManagerWithMerger);
				})
				.whenComplete(toLogger(logger, "ensureThread", threadId));
	}

	private Promise<?> removeThread(String threadId) {
		threadDaos.remove(threadId);
		Promise<StateManagerWithMerger<ThreadOperation>> stateManagerWithMergerPromise = postStateManagersWithMergers.remove(threadId);

		if (stateManagerWithMergerPromise != null) {
			StateManagerWithMerger<ThreadOperation> stateManagerWithMerger = stateManagerWithMergerPromise.getResult();
			if (stateManagerWithMerger != null) {
				return stateManagerWithMerger.stop()
						.whenComplete(toLogger(logger, "removeThread", threadId));
			}
		}
		return Promise.complete()
				.whenComplete(toLogger(logger, "removeThread", threadId));
	}

	public Map<String, Promise<ThreadDao>> getThreadDaos() {
		return threadDaos;
	}
}
