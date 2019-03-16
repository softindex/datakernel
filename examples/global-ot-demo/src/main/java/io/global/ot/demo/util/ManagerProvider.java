package io.global.ot.demo.util;


import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTState;
import io.datakernel.ot.OTStateManager;
import io.global.ot.api.CommitId;
import io.global.ot.common.DelayedPushNode;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public final class ManagerProvider<D> implements EventloopService {
	private final Map<String, MaterializedPromise<OTStateManager<CommitId, D>>> entries = new HashMap<>();

	private final OTAlgorithms<CommitId, D> algorithms;
	private final Supplier<OTState<D>> stateSupplier;

	public ManagerProvider(OTAlgorithms<CommitId, D> algorithms, Supplier<OTState<D>> stateSupplier) {
		this.algorithms = algorithms;
		this.stateSupplier = stateSupplier;
	}

	public Promise<OTStateManager<CommitId, D>> get(String id) {
		if (!entries.containsKey(id)) {
			OTStateManager<CommitId, D> stateManager = OTStateManager.create(
					algorithms.getEventloop(),
					algorithms.getOtSystem(),
					DelayedPushNode.create(algorithms.getOtNode(), Duration.ofSeconds(5)),
					stateSupplier.get())
					.withPoll();

			MaterializedPromise<OTStateManager<CommitId, D>> stateManagerPromise = stateManager.start()
					.map($ -> stateManager)
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

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return algorithms.getEventloop();
	}

	public OTAlgorithms<CommitId, D> getAlgorithms() {
		return algorithms;
	}

	@NotNull
	@Override
	public MaterializedPromise<?> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public MaterializedPromise<?> stop() {
		return Promises.all(entries.values().stream()
				.map(MaterializedPromise::getResult)
				.map(OTStateManager::stop))
				.materialize();
	}
}
