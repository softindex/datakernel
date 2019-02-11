package io.global.ot.common;


import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTState;
import io.datakernel.ot.OTStateManager;
import io.global.ot.api.CommitId;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public final class ManagerProvider<D> {
	private final Map<String, MaterializedPromise<OTStateManager<CommitId, D>>> entries = new HashMap<>();

	private final OTAlgorithms<CommitId, D> algorithms;
	private final Supplier<OTState<D>> stateSupplier;
	private final Duration syncInterval;

	public ManagerProvider(OTAlgorithms<CommitId, D> algorithms, Supplier<OTState<D>> stateSupplier, Duration syncInterval) {
		this.algorithms = algorithms;
		this.stateSupplier = stateSupplier;
		this.syncInterval = syncInterval;
	}

	public Promise<OTStateManager<CommitId, D>> get(String id) {
		if (!entries.containsKey(id)) {
			OTStateManager<CommitId, D> stateManager = new OTStateManager<>(algorithms.getEventloop(), algorithms.getOtSystem(),
					new DelayedCommitNode<>(algorithms.getOtNode(), 5_000), stateSupplier.get());

			EventloopTaskScheduler syncScheduler = EventloopTaskScheduler.create(algorithms.getEventloop(), stateManager::sync)
					.withInterval(syncInterval);

			MaterializedPromise<OTStateManager<CommitId, D>> stateManagerPromise = stateManager.checkout()
					.thenCompose($ -> syncScheduler.start())
					.thenApply($ -> stateManager)
					.materialize();

			entries.put(id, stateManagerPromise);
			return stateManagerPromise;
		} else {
			return entries.get(id);
		}
	}

	public Set<String> getIds() {
		return new HashSet<>(entries.keySet());
	}
}
