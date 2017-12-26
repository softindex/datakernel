package io.datakernel.ot;

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stages;
import io.datakernel.ot.OTLoadedGraph.MergeNode;
import io.datakernel.ot.exceptions.OTException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionStage;

import static io.datakernel.async.Stages.onError;
import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Collections.*;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toMap;

public class OTMergeAlgorithm {
	private static final Logger logger = LoggerFactory.getLogger(OTMergeAlgorithm.class);

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

	private static <K, D> Set<K> removeParents(OTLoadedGraph<K, D> graph, Set<K> nodes) {
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

	private static <K, D> List<D> findParent(OTLoadedGraph<K, D> graph, Comparator<K> nodeComparator,
	                                         K parent, K child) {
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

	public static <K, D> CompletionStage<Map<K, List<D>>> loadAndMerge(OTSystem<D> otSystem, OTRemote<K, D> remote, Comparator<K> keyComparator,
	                                                                   Set<K> heads) {
		if (heads.size() == 0) return Stages.of(emptyMap());
		if (heads.size() == 1) return Stages.of(singletonMap(first(heads), emptyList()));
		logger.info("Merging " + heads);
		return loadGraph(remote, keyComparator, heads)
				.thenCompose(graph -> {
					try {
						Map<K, List<D>> mergeResult = merge(otSystem, keyComparator, graph, heads);
						if (logger.isTraceEnabled()) {
							logger.info("Merge result " + mergeResult + "\n" + graph.toGraphViz() + "\n");
						} else {
							logger.info("Merge result " + mergeResult);
						}
						return Stages.of(mergeResult);
					} catch (OTException e) {
						if (logger.isTraceEnabled()) {
							logger.error("Merge error " + heads + "\n" + graph.toGraphViz() + "\n", e);
						} else {
							logger.error("Merge error " + heads);
						}
						return Stages.ofException(e);
					}
				});
	}

	static <K, D> CompletionStage<OTLoadedGraph<K, D>> loadGraph(OTRemote<K, D> remote, Comparator<K> keyComparator,
	                                                             Set<K> heads) {
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
		doLoadGraph(remote, graph, queue, new HashSet<>(), head2roots, root2heads, cb);

		return cb.thenApply($ -> graph)
				.whenComplete(onError(throwable -> {
					if (logger.isTraceEnabled()) {
						logger.error("loading error " + heads + "\n" + graph.toGraphViz() + "\n", throwable);
					} else {
						logger.error("loading error " + heads, throwable);
					}
				}));
	}

	private static <K, D> void doLoadGraph(OTRemote<K, D> remote, OTLoadedGraph<K, D> graph, PriorityQueue<K> queue,
	                                       Set<K> visited, Map<K, Set<K>> head2roots, Map<K, Set<K>> root2heads,
	                                       SettableStage<Void> cb) {
		while (!queue.isEmpty()) {
			K node = queue.poll();
			if (!visited.add(node)) continue;
			remote.loadCommit(node)
					.thenAcceptAsync(commit -> {
						Set<K> affectedHeads = root2heads.remove(node);
						for (K affectedHead : affectedHeads) {
							head2roots.get(affectedHead).remove(node);
						}
						for (K parent : commit.getParents().keySet()) {
							Set<K> parentRoots = findRoots(graph, parent);
							for (K affectedHead : affectedHeads) {
								head2roots.computeIfAbsent(affectedHead, $::newHashSet).addAll(parentRoots);
							}
							for (K parentRoot : parentRoots) {
								root2heads.computeIfAbsent(parentRoot, $::newHashSet).addAll(affectedHeads);
							}
						}

						for (K parent : commit.getParents().keySet()) {
							graph.add(parent, node, commit.getParents().get(parent));
							if (graph.getParents(parent) == null) {
								queue.add(parent);
							}
						}

						boolean found = affectedHeads.stream()
								.anyMatch(affectedHead -> head2roots.get(affectedHead).equals(root2heads.keySet()));
						if (found) {
							cb.set(null);
						} else {
							doLoadGraph(remote, graph, queue, visited, head2roots, root2heads, cb);
						}
					})
					.whenComplete(onError(cb::setException));
			return;
		}
		cb.setException(new OTException("Incomplete graph"));
	}

	static <K, D> Map<K, List<D>> merge(OTSystem<D> otSystem, Comparator<K> keyComparator,
	                                    OTLoadedGraph<K, D> graph, Set<K> nodes) throws OTException {
		checkArgument(nodes.size() >= 2);
		Comparator<K> nodeComparator = (node1, node2) -> -OTLoadedGraph.compareNodes(node1, node2, keyComparator);
		K mergeNode = doMerge(otSystem, nodeComparator, graph, removeParents(graph, nodes));
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

	private final static class RootNodes<K> {
		final K node;
		final int rootNodes;

		RootNodes(K node, int rootNodes) {
			this.node = node;
			this.rootNodes = rootNodes;
		}
	}

	@SuppressWarnings({"unchecked", "ConstantConditions"})
	private static <K, D> K doMerge(OTSystem<D> otSystem, Comparator<K> nodeComparator, OTLoadedGraph<K, D> graph,
	                                Set<K> nodes) throws OTException {
		if (nodes.size() == 1) return first(nodes);

		K pivotNode = nodes.stream()
				.map(node -> new RootNodes<>(node, findRoots(graph, node).size()))
				.min(comparingInt(v -> v.rootNodes))
				.get().node;

		Map<K, List<D>> pivotNodeParents = graph.getParents(pivotNode);
		Set<K> recursiveMergeNodes = union(pivotNodeParents.keySet(), difference(nodes, singleton(pivotNode)));
		K mergeNode = doMerge(otSystem, nodeComparator, graph, removeParents(graph, recursiveMergeNodes));
		K parentNode = first(pivotNodeParents.keySet());
		List<D> parentToPivotNode = pivotNodeParents.get(parentNode);
		List<D> parentToMergeNode = findParent(graph, nodeComparator, parentNode, mergeNode);

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
