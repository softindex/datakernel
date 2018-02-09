package io.datakernel.ot;

import io.datakernel.async.Callback;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.ot.OTLoadedGraph.MergeNode;
import io.datakernel.ot.exceptions.OTException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Collections.*;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toMap;

final class OTMergeAlgorithm<K, D> {
	private static final Logger logger = LoggerFactory.getLogger(OTMergeAlgorithm.class);

	private final OTSystem<D> otSystem;
	private final OTRemote<K, D> remote;
	private final Comparator<K> keyComparator;
	private final Comparator<K> nodeComparator;

	public OTMergeAlgorithm(OTSystem<D> otSystem, OTRemote<K, D> remote, Comparator<K> keyComparator) {
		this.otSystem = otSystem;
		this.remote = remote;
		this.keyComparator = keyComparator;
		this.nodeComparator = (node1, node2) -> -OTLoadedGraph.compareNodes(node1, node2, keyComparator);
	}

	private static <K> Set<K> findRoots(OTLoadedGraph<K, ?> graph, K node) {
		Set<K> result = new HashSet<>();
		Set<K> visited = new HashSet<>();
		ArrayList<K> queue = new ArrayList<>(singletonList(node));
		while (!queue.isEmpty()) {
			K node1 = queue.remove(queue.size() - 1);
			if (!visited.add(node1)) continue;
			Map<K, ? extends List<?>> parents = graph.getParents(node1);
			if (parents == null) {
				result.add(node1);
			} else {
				queue.addAll(parents.keySet());
			}
		}
		return result;
	}

	private static <K, D> Set<K> excludeParents(OTLoadedGraph<K, D> graph, Set<K> nodes) {
		Set<K> result = new LinkedHashSet<>(nodes);
		if (result.size() <= 1) return result;
		Set<K> visited = new HashSet<>();
		ArrayList<K> queue = new ArrayList<>(nodes);
		while (!queue.isEmpty()) {
			K node = queue.remove(queue.size() - 1);
			if (!visited.add(node)) continue;
			for (K parent : nullToEmpty(graph.getParents(node)).keySet()) {
				result.remove(parent);
				if (!visited.contains(parent)) {
					queue.add(parent);
				}
			}
		}
		return result;
	}

	private List<D> findParent(OTLoadedGraph<K, D> graph, K parent, K child) {
		if (child.equals(parent)) return emptyList();
		Set<K> visited = new HashSet<>();
		PriorityQueue<K> queue = new PriorityQueue<>(nodeComparator);
		queue.add(child);
		Map<K, List<D>> result = new HashMap<>();
		result.put(child, emptyList());
		while (!queue.isEmpty()) {
			K node = queue.poll();
			List<D> node2child = result.remove(node);
			if (!visited.add(node)) continue;
			assert node2child != null;
			Map<K, List<D>> nodeParents = nullToEmpty(graph.getParents(node));
			for (K nodeParent : nodeParents.keySet()) {
				if (!visited.contains(nodeParent) && !result.containsKey(nodeParent)) {
					List<D> parent2child = concat(nodeParents.get(nodeParent), node2child);
					if (nodeParent.equals(parent)) return parent2child;
					result.put(nodeParent, parent2child);
					queue.add(nodeParent);
				}
			}
		}
		throw new AssertionError();
	}

	public Stage<Map<K, List<D>>> loadAndMerge(Set<K> heads) {
		if (heads.size() == 0) return Stage.of(emptyMap());
		if (heads.size() == 1) return Stage.of(singletonMap(first(heads), emptyList()));
		logger.info("Merging " + heads);
		return loadGraph(heads)
				.thenCompose(graph -> {
					try {
						Map<K, List<D>> mergeResult = merge(graph, heads);
						if (logger.isTraceEnabled()) {
							logger.info("Merge result " + mergeResult + "\n" + graph.toGraphViz() + "\n");
						} else {
							logger.info("Merge result " + mergeResult);
						}
						return Stage.of(mergeResult);
					} catch (OTException e) {
						if (logger.isTraceEnabled()) {
							logger.error("Merge error " + heads + "\n" + graph.toGraphViz() + "\n", e);
						} else {
							logger.error("Merge error " + heads);
						}
						return Stage.ofException(e);
					}
				});
	}

	Stage<OTLoadedGraph<K, D>> loadGraph(Set<K> heads) {
		checkArgument(heads.size() >= 2);

		OTLoadedGraph<K, D> graph = new OTLoadedGraph<>();
		PriorityQueue<K> queue = new PriorityQueue<>(keyComparator.reversed());
		queue.addAll(heads);

		Map<K, Set<K>> head2roots = new HashMap<>();
		Map<K, Set<K>> root2heads = new HashMap<>();
		for (K head : heads) {
			head2roots.put(head, set(head));
			root2heads.put(head, set(head));
		}

		SettableStage<Void> cb = SettableStage.create();
		doLoadGraph(graph, queue, new HashSet<>(), head2roots, root2heads, cb);

		return cb.thenApply($ -> graph)
				.whenException(throwable -> {
					if (logger.isTraceEnabled()) {
						logger.error("loading error " + heads + "\n" + graph.toGraphViz() + "\n", throwable);
					} else {
						logger.error("loading error " + heads, throwable);
					}
				});
	}

	private void doLoadGraph(OTLoadedGraph<K, D> graph, PriorityQueue<K> queue,
	                         Set<K> visited, Map<K, Set<K>> head2roots, Map<K, Set<K>> root2heads,
	                         Callback<Void> cb) {
		while (!queue.isEmpty()) {
			K node = queue.poll();
			if (!visited.add(node)) continue;
			remote.loadCommit(node)
					.thenAccept(commit -> {
						if (commit.isRoot()) {
							cb.setException(new OTException("Trying to load past root"));
							return;
						}
						Map<K, List<D>> parents = commit.getParents();

						Set<K> affectedHeads = root2heads.remove(node);
						for (K affectedHead : affectedHeads) {
							head2roots.get(affectedHead).remove(node);
						}
						Set<K> affectedRoots = new HashSet<>();
						for (K parent : parents.keySet()) {
							Set<K> parentRoots = findRoots(graph, parent);
							for (K affectedHead : affectedHeads) {
								head2roots.computeIfAbsent(affectedHead, $ -> new HashSet<>()).addAll(parentRoots);
							}
							for (K parentRoot : parentRoots) {
								root2heads.computeIfAbsent(parentRoot, $ -> new HashSet<>()).addAll(affectedHeads);
								affectedRoots.add(parentRoot);
							}
						}

						for (K parent : parents.keySet()) {
							graph.add(parent, node, parents.get(parent));
							if (graph.getParents(parent) == null) {
								queue.add(parent);
							}
						}

						boolean found = affectedRoots.stream()
								.flatMap(affectedRoot -> root2heads.get(affectedRoot).stream()).distinct()
								.anyMatch(affectedHead -> head2roots.get(affectedHead).equals(root2heads.keySet()));
						if (found) {
							cb.set(null);
						} else {
							doLoadGraph(graph, queue, visited, head2roots, root2heads, cb);
						}
					})
					.whenException(cb::setException);
			return;
		}
		cb.setException(new OTException("Incomplete graph"));
	}

	Map<K, List<D>> merge(OTLoadedGraph<K, D> graph, Set<K> nodes) throws OTException {
		checkArgument(nodes.size() >= 2);
		K mergeNode = doMerge(graph, excludeParents(graph, nodes));
		assert mergeNode != null;
		PriorityQueue<K> queue = new PriorityQueue<>(nodeComparator);
		queue.add(mergeNode);
		Map<K, List<D>> paths = new HashMap<>();
		Map<K, List<D>> result = new HashMap<>();
		paths.put(mergeNode, emptyList());
		Set<K> visited = new HashSet<>();
		while (!queue.isEmpty()) {
			K node = queue.poll();
			List<D> path = paths.remove(node);
			if (!visited.add(node)) continue;
			if (nodes.contains(node)) {
				result.put(node, path);
				if (result.size() == nodes.size()) {
					break;
				}
			}
			Map<K, List<D>> parentsMap = nullToEmpty(graph.getParents(node));
			for (K parent : parentsMap.keySet()) {
				if (visited.contains(parent) || paths.containsKey(parent)) continue;
				paths.put(parent, concat(parentsMap.get(parent), path));
				queue.add(parent);
			}
		}
		assert result.size() == nodes.size();
		return result.entrySet().stream()
				.collect(toMap(Map.Entry::getKey, ops -> otSystem.squash(ops.getValue())));
	}

	@SuppressWarnings({"unchecked", "ConstantConditions"})
	private K doMerge(OTLoadedGraph<K, D> graph,
	                  Set<K> nodes) throws OTException {
		if (nodes.size() == 1) return first(nodes);

		K pivotNode = nodes.stream().min(comparingInt((K node) -> findRoots(graph, node).size())).get();

		Map<K, List<D>> pivotNodeParents = graph.getParents(pivotNode);
		Set<K> recursiveMergeNodes = union(pivotNodeParents.keySet(), difference(nodes, singleton(pivotNode)));
		K mergeNode = doMerge(graph, excludeParents(graph, recursiveMergeNodes));
		K parentNode = first(pivotNodeParents.keySet());
		List<D> parentToPivotNode = pivotNodeParents.get(parentNode);
		List<D> parentToMergeNode = findParent(graph, parentNode, mergeNode);

		if (pivotNodeParents.size() > 1) {
			K resultNode = (K) new MergeNode();
			graph.add(mergeNode, resultNode, emptyList());
			graph.add(pivotNode, resultNode,
					otSystem.squash(concat(otSystem.invert(parentToPivotNode), parentToMergeNode)));
			return resultNode;
		}

		if (pivotNodeParents.size() == 1) {
			TransformResult<D> transformed = otSystem.transform(parentToPivotNode, parentToMergeNode);
			K resultNode = (K) new MergeNode();
			graph.add(mergeNode, resultNode, transformed.right);
			graph.add(pivotNode, resultNode, transformed.left);
			return resultNode;
		}

		throw new OTException("Graph cannot be merged");
	}

}
