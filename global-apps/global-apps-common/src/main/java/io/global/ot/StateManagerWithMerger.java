package io.global.ot;

import io.datakernel.async.service.EventloopService;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.global.ot.api.CommitId;
import io.global.ot.client.MergeService;
import org.jetbrains.annotations.NotNull;

public final class StateManagerWithMerger<D> implements EventloopService {
	private final OTStateManager<CommitId, D> stateManager;
	private final MergeService<CommitId, D> mergeService;

	private StateManagerWithMerger(OTStateManager<CommitId, D> stateManager, MergeService<CommitId, D> mergeService) {
		this.stateManager = stateManager;
		this.mergeService = mergeService;
	}

	public static <D> StateManagerWithMerger<D> create(OTStateManager<CommitId, D> stateManager, MergeService<CommitId, D> mergeService) {
		return new StateManagerWithMerger<D>(stateManager, mergeService);
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return stateManager.getEventloop();
	}

	public OTStateManager<CommitId, D> getStateManager() {
		return stateManager;
	}

	public MergeService<CommitId, D> getMergeService() {
		return mergeService;
	}

	@Override
	public @NotNull Promise<?> start() {
		return stateManager.start()
				.then($ -> mergeService.start());
	}

	@Override
	public @NotNull Promise<?> stop() {
		return stateManager.stop()
				.then($ -> mergeService.stop());
	}
}
