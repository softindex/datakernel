/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.ot.counter.state;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.HttpRequest;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.counter.operations.Operation;
import io.datakernel.ot.counter.operations.OperationState;
import io.global.ot.api.CommitId;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static io.datakernel.ot.counter.util.Utils.COMMIT_ID_HASH;
import static io.datakernel.util.Preconditions.checkState;

public class StateManagerProvider {
	private final Eventloop eventloop;
	private final OTAlgorithms<CommitId, Operation> algorithms;
	private final Map<Integer, OTStateManager<CommitId, Operation>> stateManagerMap = new HashMap<>();
	private final Map<OTStateManager<CommitId, Operation>, NodesWalker<CommitId>> walkerMap = new HashMap<>();

	// shows whether last get operation returned new StateManager or not
	private boolean isNew;

	public StateManagerProvider(Eventloop eventloop, OTAlgorithms<CommitId, Operation> algorithms) {
		this.eventloop = eventloop;
		this.algorithms = algorithms;
	}

	public Promise<OTStateManager<CommitId, Operation>> get(HttpRequest request) {
		Integer id = getId(request);
		isNew = !stateManagerMap.containsKey(id);
		OTStateManager<CommitId, Operation> stateManager = stateManagerMap.computeIfAbsent(id,
				$ -> OTStateManager.create(eventloop, algorithms, new OperationState()));
		return isNew ?
				stateManager.start()
						.thenCompose($ -> getWalker(stateManager).walkFull())
						.thenApply($ -> stateManager) :
				Promise.of(stateManager);
	}

	public boolean isNew() {
		return isNew;
	}

	@Nullable
	public Integer getId(OTStateManager<CommitId, Operation> manager) {
		for (Map.Entry<Integer, OTStateManager<CommitId, Operation>> entry : stateManagerMap.entrySet()) {
			if (entry.getValue().equals(manager)) {
				return entry.getKey();
			}
		}
		throw new NoSuchElementException();
	}

	public NodesWalker<CommitId> getWalker(OTStateManager<CommitId, Operation> manager) {
		return walkerMap.computeIfAbsent(manager, $ -> new NodesWalker<>(manager, COMMIT_ID_HASH));
	}

	private Integer getId(HttpRequest request) {
		String idString = request.getQueryParameterOrNull("id");

		if (idString != null && !idString.isEmpty()) {
			Integer id = Integer.valueOf(idString);
			checkState(id >= 0);
			return id;
		} else {
			return stateManagerMap.keySet().stream()
					.sorted()
					.reduce(0, (acc, next) -> !next.equals(acc) ? acc : next + 1);
		}
	}
}
