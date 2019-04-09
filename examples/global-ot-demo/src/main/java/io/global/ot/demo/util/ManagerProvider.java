package io.global.ot.demo.util;


import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.ot.*;
import io.global.common.ot.DelayedPushNode;
import io.global.ot.api.CommitId;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public final class ManagerProvider<D> implements EventloopService {
	private final Map<String, MaterializedPromise<OTStateManager<CommitId, D>>> entries = new HashMap<>();

	private final Eventloop eventloop;
	private final OTNode<CommitId, D, OTCommit<CommitId, D>> node;
	private final OTSystem<D> system;
	private final Supplier<OTState<D>> stateSupplier;
	private final Duration delay;

	public ManagerProvider(Eventloop eventloop, OTNode<CommitId, D, OTCommit<CommitId, D>> node,
			OTSystem<D> system, Supplier<OTState<D>> stateSupplier, Duration delay) {
		this.eventloop = eventloop;
		this.node = node;
		this.system = system;
		this.stateSupplier = stateSupplier;
		this.delay = delay;
	}

	public Promise<OTStateManager<CommitId, D>> get(String id) {
		if (!entries.containsKey(id)) {
			OTStateManager<CommitId, D> stateManager = OTStateManager.create(
					eventloop,
					system,
					DelayedPushNode.create(node, delay),
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
		return eventloop;
	}

	public OTSystem<D> getSystem() {
		return system;
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
