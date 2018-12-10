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

package io.global.ot.demo.state;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.async.SettablePromise;
import io.datakernel.codec.StructuredEncoder;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRepository;
import io.datakernel.ot.OTStateManager;
import io.global.ot.demo.operations.Operation;
import io.global.ot.demo.operations.OperationState;

import java.util.*;

import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.util.CollectionUtils.difference;
import static io.datakernel.util.CollectionUtils.first;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.joining;

public class NodesWalker<T> {
	private final OTGraph graph;
	private final OTStateManager<T, Operation> stateManager;
	private final OTRepository<T, Operation> repository;
	private final OperationState state = new OperationState();

	private T revision;
	@Nullable
	private T fetchedRevision;

	NodesWalker(OTStateManager<T, Operation> stateManager, StructuredEncoder<T> encoder) {
		this.stateManager = stateManager;
		this.graph = new OTGraph(encoder);
		this.repository = stateManager.getAlgorithms().getRepository();
	}

	public Promise<Void> walk() {
		//noinspection OptionalGetWithoutIsPresent
		T current = stateManager.hasPendingCommits() ?
				first(stateManager.getPendingCommits().values()
						.stream()
						.min(OTCommit::compareTo)
						.get()
						.getParents()
						.keySet()) :
				stateManager.getRevision();
		return walk(singleton(current));
	}

	public Promise<Void> walkFull() {
		return stateManager.getAlgorithms().getRepository().getHeads()
				.thenCompose(this::walk);
	}

	public Promise<Void> walk(Set<T> heads) {
		return Promises.toList(heads.stream().map(repository::loadCommit))
				.thenCompose(commits -> {
					SettablePromise<Void> cb = new SettablePromise<>();
					Queue<OTCommit<T, Operation>> queue = new PriorityQueue<>(commits);
					walkImpl(queue, cb);
					return cb;
				});
	}

	private void walkImpl(Queue<OTCommit<T, Operation>> queue, SettablePromise<Void> cb) {
		if (queue.isEmpty()) {
			cb.set(null);
			return;
		}

		OTCommit<T, Operation> commit = queue.peek();
		graph.visit(commit);

		if (commit.isRoot()) {
			state.init();
			graph.setState(commit.getId(), state.getCounter());
		}

		Map<T, List<Operation>> parents = commit.getParents();
		for (T id : parents.keySet()) {
			Integer maybeState = graph.tryGetState(id);
			if (maybeState != null) {
				state.setCounter(maybeState);
				parents.get(id).forEach(state::apply);
				graph.setState(commit.getId(), state.getCounter());
				break;
			}
		}
		if (graph.tryGetState(commit.getId()) != null) {
			queue.remove(commit);
			if (graph.allParentsVisited(commit)) {
				walkImpl(queue, cb);
				return;
			}
		}

		Promises.toList(
				commit.getParents().keySet().stream()
						.filter(id -> !graph.hasBeenVisited(id))
						.map(repository::loadCommit))
				.thenApply(queue::addAll)
				.whenResult($ -> walkImpl(queue, cb));
	}

	public String toGraphViz() {
		return graph.toGraphViz();
	}

	private final class OTGraph {
		private final Set<T> visited = new HashSet<>();
		private final Map<T, Map<T, List<Operation>>> child2parent = new HashMap<>();
		private final Map<T, Map<T, List<Operation>>> parent2child = new HashMap<>();
		private final Map<T, Integer> commitStateMap = new HashMap<>();
		private final OperationState state = new OperationState();
		private final Map<T, OTCommit<T, Operation>> pendingCommits;
		private final StructuredEncoder<T> encoder;

		private OTGraph(StructuredEncoder<T> encoder) {
			this.pendingCommits = stateManager.getPendingCommits();
			this.encoder = encoder;
		}

		private void visit(OTCommit<T, Operation> commit) {
			if (!visited.contains(commit.getId())) {
				visited.add(commit.getId());
				commit.getParents().forEach((key, value) -> graph.addEdge(key, commit.getId(), value));
			}
		}

		private boolean hasBeenVisited(T id) {
			return visited.contains(id);
		}

		private boolean allParentsVisited(OTCommit<T, Operation> child) {
			for (T parent : child.getParents().keySet()) {
				if (!visited.contains(parent)) {
					return false;
				}
			}
			return true;
		}

		@Nullable
		private Integer tryGetState(T id) {
			return commitStateMap.get(id);
		}

		private void setState(T id, Integer state) {
			commitStateMap.put(id, state);
		}

		private void addEdge(T parent, T child, List<Operation> diff) {
			child2parent.computeIfAbsent(child, $ -> new HashMap<>()).put(parent, diff);
			parent2child.computeIfAbsent(parent, $ -> new HashMap<>()).put(child, diff);
		}

		private T getRoot() {
			Set<T> roots = difference(parent2child.keySet(), child2parent.keySet());
			return roots.isEmpty() ?
					first(visited) :
					first(roots);
		}

		private Set<T> getTips() {
			return difference(child2parent.keySet(), parent2child.keySet());
		}

		private String toGraphViz() {
			pendingCommits.values().stream()
					.sorted(Comparator.comparingLong(OTCommit::getTimestamp))
					.forEach(this::updatePendingState);
			revision = stateManager.getRevision();
			fetchedRevision = stateManager.getFetchedRevisionOrNull();
			StringBuilder sb = new StringBuilder();
			sb.append("digraph {\n");
			for (T child : child2parent.keySet()) {
				Map<T, List<Operation>> parent2diffs = child2parent.get(child);
				String color = (parent2diffs.size() == 1) ? "color=blue; " : "";
				for (T parent : parent2diffs.keySet()) {
					List<Operation> diffs = parent2diffs.get(parent);
					sb.append("\t")
							.append(nodeStateToGraphViz(child))
							.append(" -> ").append(nodeStateToGraphViz(parent))
							.append(" [ dir=\"back\"; ").append(color).append("label=\"")
							.append(diffsToGraphViz(diffs))
							.append("\"];\n");
				}
				addTooltipAndStyle(sb, child);
			}
			for (OTCommit<T, Operation> value : pendingCommits.values()) {
				sb.append('\t')
						.append(nodeStateToGraphViz(value.getId()))
						.append(" -> ")
						.append(nodeStateToGraphViz(first(value.getParentIds())))
						.append(" [dir = \"back\"; style=dashed; label=\"")
						.append(diffsToGraphViz(first(value.getParents().values())))
						.append("\"];\n");
				addTooltipAndStyle(sb, value.getId());
			}

			// if (pendingCommits.isEmpty()) {
			addTooltipAndStyle(sb, getRoot());
			// }

			sb.append("\t{ rank=same; ")
					.append(getTips().stream().map(this::nodeStateToGraphViz).collect(joining(" ")))
					.append(" };\n");
			sb.append("\t{ rank=same; ")
					.append(nodeStateToGraphViz(getRoot()))
					.append(" };\n");
			sb.append("}\n");

			return sb.toString();
		}

		private void addTooltipAndStyle(StringBuilder sb, T node) {
			boolean isPending = pendingCommits.containsKey(node);
			boolean isCurrent = node.equals(revision);
			boolean isFetched = node.equals(fetchedRevision);
			sb.append("\t")
					.append(nodeStateToGraphViz(node))
					.append(" [tooltip=")
					.append(toJson(encoder, node))
					.append(" style=")
					.append(isPending ? "dashed color=" : "filled fillcolor=")
					.append(isFetched ?
							"yellow" :
							isCurrent ?
									"green" :
									"white")
					.append("];\n");
		}

		private void updatePendingState(OTCommit<T, Operation> commit) {
			if (commitStateMap.containsKey(commit.getId())) {
				return;
			}
			Map.Entry<T, List<Operation>> first = first(commit.getParents().entrySet());
			state.setCounter(commitStateMap.get(first.getKey()));
			first.getValue().forEach(state::apply);
			commitStateMap.put(commit.getId(), state.getCounter());
		}

		private String diffsToGraphViz(Collection<Operation> diffs) {
			return diffs.isEmpty() ? "∅" : diffs.size() == 1 ? first(diffs).toString() : diffs.toString();
		}

		private String nodeStateToGraphViz(T node) {
			return toJson(encoder, node).substring(0, 8) + ":[" + commitStateMap.getOrDefault(node, 0) + "]\"";
		}
	}

}
