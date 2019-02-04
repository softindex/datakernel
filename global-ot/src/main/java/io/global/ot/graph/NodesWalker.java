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

package io.global.ot.graph;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.async.SettablePromise;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRepository;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;

import static io.datakernel.util.CollectionUtils.difference;
import static io.datakernel.util.CollectionUtils.first;
import static java.util.stream.Collectors.joining;

public final class NodesWalker<K, D> {
	private final OTGraph<K, D> graph;
	private final OTRepository<K, D> repository;

	@Nullable
	private K revision;

	private NodesWalker(OTRepository<K, D> repository, Function<K, String> idToString, Function<D, String> diffToString) {
		this.repository = repository;
		this.graph = new OTGraph<>(idToString, diffToString);
	}

	public static <K, D> NodesWalker<K, D> create(OTRepository<K, D> repository, Function<K, String> idToString, Function<D, String> diffToString) {
		return new NodesWalker<>(repository, idToString, diffToString);
	}

	public Promise<Void> walk() {
		return repository.getHeads()
				.thenCompose(this::walk);
	}

	public Promise<Void> walk(@Nullable K revision) {
		return repository.getHeads()
				.thenCompose(heads -> walk(heads, revision));
	}

	public Promise<Void> walk(Set<K> heads) {
		return walk(heads, null);
	}

	public Promise<Void> walk(Set<K> heads, @Nullable K revision) {
		this.revision = revision == null ? first(heads) : revision;
		return Promises.toList(heads.stream().map(repository::loadCommit))
				.thenCompose(commits -> {
					SettablePromise<Void> cb = new SettablePromise<>();
					Queue<OTCommit<K, D>> queue = new PriorityQueue<>(commits);
					walkImpl(queue, cb);
					return cb;
				});
	}

	private void walkImpl(Queue<OTCommit<K, D>> queue, SettablePromise<Void> cb) {
		if (queue.isEmpty()) {
			cb.set(null);
			return;
		}

		OTCommit<K, D> commit = queue.poll();
		graph.visit(commit);

		Promises.toList(
				commit.getParents().keySet().stream()
						.filter(id -> !graph.hasBeenVisited(id))
						.map(repository::loadCommit))
				.thenApply(queue::addAll)
				.whenResult($ -> walkImpl(queue, cb));
	}

	public String toGraphViz() {
		return graph.toGraphViz(revision);
	}

	private static final class OTGraph<K, D> {
		private final Map<K, Map<K, List<D>>> child2parent = new HashMap<>();
		private final Map<K, Map<K, List<D>>> parent2child = new HashMap<>();
		private final Function<K, String> idToString;
		private final Function<D, String> diffToString;

		private OTGraph(Function<K, String> idToString, Function<D, String> diffToString) {
			this.idToString = idToString;
			this.diffToString = diffToString;
		}

		private void visit(OTCommit<K, D> commit) {
			Map<K, List<D>> parents = commit.getParents();
			if (parents.keySet().isEmpty()) {
				// rootCommit
				parent2child.computeIfAbsent(commit.getId(), k -> new HashMap<>());
				return;
			}
			parents.forEach((key, value) -> addEdge(key, commit.getId(), value));
		}

		private boolean hasBeenVisited(K id) {
			return child2parent.keySet().contains(id);
		}

		private void addEdge(K parent, K child, List<D> diff) {
			child2parent.computeIfAbsent(child, $ -> new HashMap<>()).put(parent, diff);
			parent2child.computeIfAbsent(parent, $ -> new HashMap<>()).put(child, diff);
		}

		private K getRoot() {
			Set<K> roots = difference(parent2child.keySet(), child2parent.keySet());
			return roots.isEmpty() ?
					first(parent2child.keySet()) :
					first(roots);
		}

		private Set<K> getTips() {
			return difference(child2parent.keySet(), parent2child.keySet());
		}

		private String toGraphViz(@Nullable K revision) {
			StringBuilder sb = new StringBuilder();
			sb.append("digraph {\n");
			for (K child : child2parent.keySet()) {
				Map<K, List<D>> parent2diffs = child2parent.get(child);
				String color = (parent2diffs.size() == 1) ? "color=blue; " : "";
				for (K parent : parent2diffs.keySet()) {
					List<D> diffs = parent2diffs.get(parent);
					sb.append("\t")
							.append(nodeToGraphViz(child))
							.append(" -> ").append(nodeToGraphViz(parent))
							.append(" [ dir=\"back\"; ").append(color).append("label=\"")
							.append(diffsToGraphViz(diffs))
							.append("\"];\n");
				}
				addStyle(sb, child, revision);
			}

			addStyle(sb, getRoot(), revision);

			sb.append("\t{ rank=same; ")
					.append(getTips().stream().map(this::nodeToGraphViz).collect(joining(" ")))
					.append(" };\n");
			sb.append("\t{ rank=same; ")
					.append(nodeToGraphViz(getRoot()))
					.append(" };\n");
			sb.append("}\n");

			return sb.toString();
		}

		private void addStyle(StringBuilder sb, K node, @Nullable K revision) {
			sb.append("\t")
					.append(nodeToGraphViz(node))
					.append(" [style=filled fillcolor=")
					.append(node.equals(revision) ? "green" : "white")
					.append("];\n");
		}

		private String nodeToGraphViz(K node) {
			return "\"" + idToString.apply(node) + "\"";
		}

		private String diffsToGraphViz(List<D> diffs) {
			return diffs.isEmpty() ? "∅" : diffs.stream().map(diffToString).collect(joining(",\n"));
		}
	}
}
