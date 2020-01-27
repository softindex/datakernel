package io.global.notes;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.Key;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.ot.StateManagerWithMerger;
import io.global.ot.TypedRepoNames;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.client.MergeService;
import io.global.ot.edit.EditOperation;
import io.global.ot.map.MapOTStateListenerProxy;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.service.AbstractUserContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.datakernel.async.util.LogUtils.toLogger;

public final class GlobalNotesContainer extends AbstractUserContainer {
	private static final Logger logger = LoggerFactory.getLogger(GlobalNotesContainer.class);

	private final StateManagerWithMerger<MapOperation<String, String>> stateManagerWithMerger;
	private final MapOTStateListenerProxy<String, String> notesState;

	@Inject
	private Function<String, MergeService<CommitId, EditOperation>> mergeServiceFactory;
	@Inject
	private GlobalOTNodeImpl node;
	@Inject
	TypedRepoNames names;

	private final Map<String, Promise<MergeService<CommitId, EditOperation>>> mergers = new HashMap<>();

	@Inject
	public GlobalNotesContainer(StateManagerWithMerger<MapOperation<String, String>> stateManagerWithMerger) {
		this.stateManagerWithMerger = stateManagerWithMerger;
		this.notesState = (MapOTStateListenerProxy<String, String>) stateManagerWithMerger.getStateManager().getState();
	}

	@Override
	protected Promise<?> doStart() {
		return node.fetch(RepoID.of(getKeys(), names.getRepoName(new Key<MapOperation<String, String>>(){})))
				.thenEx(($, e) -> stateManagerWithMerger.start())
				.then($ -> Promises.all(notesState
						.getMap()
						.keySet()
						.stream()
						.map(noteId -> node.fetch(RepoID.of(getKeys(), names.getRepoPrefix(Key.of(EditOperation.class)) + noteId))
								.thenEx(($2, e) -> ensureMerger(noteId)))))
				.whenResult($ -> notesState.onOperationReceived(op -> {
					Map<String, SetValue<String>> operations = op.getOperations();
					operations.forEach((id, setValue) -> {
						if (setValue.getNext() == null) {
							logger.info("Removing merger {}", setValue.getPrev());
							removeMerger(id);
						} else if (setValue.getPrev() == null) {
							logger.info("Adding merger {}", setValue.getNext());
							ensureMerger(id);
						}
					});
				}))
				.whenComplete(toLogger(logger, "start"));
	}

	@Override
	protected Promise<?> doStop() {
		return stateManagerWithMerger.stop()
				.then($ -> Promises.all(mergers.values().stream().map(promise -> promise.then(MergeService::stop))))
				.whenResult($ -> mergers.clear())
				.whenComplete(toLogger(logger, "stop"));
	}

	private Promise<MergeService<CommitId, EditOperation>> ensureMerger(String noteId) {
		return mergers.computeIfAbsent(noteId,
				nid -> {
					MergeService<CommitId, EditOperation> mergeService = mergeServiceFactory.apply(nid);
					return mergeService.start()
							.map($2 -> mergeService);
				})
				.whenComplete(toLogger(logger, "ensureMerger", noteId));
	}

	private Promise<?> removeMerger(String noteId) {
		Promise<MergeService<CommitId, EditOperation>> mergeServicePromise = mergers.remove(noteId);

		if (mergeServicePromise != null) {
			MergeService<CommitId, EditOperation> mergeService = mergeServicePromise.getResult();
			if (mergeService != null) {
				return mergeService.stop()
						.whenComplete(toLogger(logger, "removeMerger", noteId));
			}
		}
		return Promise.complete()
				.whenComplete(toLogger(logger, "removeMerger", noteId));
	}

}
