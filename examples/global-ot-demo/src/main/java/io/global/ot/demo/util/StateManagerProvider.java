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

package io.global.ot.demo.util;

import io.datakernel.async.Promise;
import io.datakernel.http.HttpRequest;
import io.datakernel.ot.OTNode;
import io.datakernel.ot.OTRepository;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.global.ot.api.CommitId;
import io.global.ot.demo.operations.Operation;
import io.global.ot.demo.operations.OperationState;
import io.global.ot.graph.NodesWalker;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static io.datakernel.util.Preconditions.checkState;
import static io.global.ot.demo.util.Utils.ID_TO_STRING;
import static io.global.ot.demo.util.Utils.OPERATION_TO_STRING;

public final class StateManagerProvider {
	private final OTSystem<Operation> otSystem;
	private final OTNode<CommitId, Operation> otNode;
	private final OTRepository<CommitId, Operation> otRepository;
	private final Map<Integer, OTStateManager<CommitId, Operation>> stateManagerMap = new HashMap<>();
	private final Map<OTStateManager<CommitId, Operation>, NodesWalker<CommitId, Operation>> walkerMap = new HashMap<>();

	// shows whether last get operation returned new StateManager or not
	private boolean isNew;

	public StateManagerProvider(OTSystem<Operation> otSystem, OTNode<CommitId, Operation> otNode, OTRepository<CommitId, Operation> otRepository) {
		this.otSystem = otSystem;
		this.otNode = otNode;
		this.otRepository = otRepository;
	}

	public Promise<OTStateManager<CommitId, Operation>> get(HttpRequest request) {
		Integer id = getId(request);
		isNew = !stateManagerMap.containsKey(id);
		OTStateManager<CommitId, Operation> stateManager = stateManagerMap.computeIfAbsent(id,
				$ -> new OTStateManager<>(otSystem, otNode, new OperationState()));
		return isNew ?
				stateManager.checkout()
						.thenCompose($ -> getWalker(stateManager).walk())
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

	public NodesWalker<CommitId, Operation> getWalker(OTStateManager<CommitId, Operation> manager) {
		return walkerMap.computeIfAbsent(manager, $ -> NodesWalker.create(otRepository, ID_TO_STRING, OPERATION_TO_STRING));
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
