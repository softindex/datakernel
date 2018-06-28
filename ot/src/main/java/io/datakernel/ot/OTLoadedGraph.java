package io.datakernel.ot;

import io.datakernel.ot.exceptions.OTException;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Collections.*;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.*;

final class OTLoadedGraph<K, D> {
	private final AtomicLong mergeId = new AtomicLong();

	public OTLoadedGraph(OTSystem<D> otSystem) {
		this.otSystem = otSystem;
	}

	private static final class MergeNode {
		final long n;

		private MergeNode(long n) {
			this.n = n;
		}

		@Override
		public String toString() {
			return "@" + n;
		}
	}

	private int compareNodes(K node1, K node2) {
		if (node1 instanceof OTLoadedGraph.MergeNode) {
			if (node2 instanceof OTLoadedGraph.MergeNode) {
				return Long.compare(((MergeNode) node1).n, ((MergeNode) node2).n);
			} else {
				return +1;
			}
		} else {
			if (node2 instanceof OTLoadedGraph.MergeNode) {
				return -1;
			} else {
				Long timestamp1 = timestamps.getOrDefault(node1, 0L);
				Long timestamp2 = timestamps.getOrDefault(node2, 0L);
				return Long.compare(timestamp1, timestamp2);
			}
		}
	}

	private final OTSystem<D> otSystem;

	private final Map<K, Long> timestamps = new HashMap<>();
	private final Map<K, Map<K, List<D>>> child2parent = new HashMap<>();
	private final Map<K, Map<K, List<D>>> parent2child = new HashMap<>();

	public void setNodeTimestamp(K node, long timestamp) {
		timestamps.put(node, timestamp);
	}

	public void addEdge(K parent, K child, List<D> diff) {
		child2parent.computeIfAbsent(child, $ -> new HashMap<>()).put(parent, diff);
		parent2child.computeIfAbsent(parent, $ -> new HashMap<>()).put(child, diff);
	}

	public Map<K, List<D>> getParents(K child) {
		return child2parent.get(child);
	}

	public Set<K> getRoots() {
		return difference(parent2child.keySet(), child2parent.keySet());
	}

	public Set<K> getTips() {
		return difference(child2parent.keySet(), parent2child.keySet());
	}

	public Set<K> getOriginalTips() {
		return child2parent.keySet().stream()
				.filter(node -> !(node instanceof MergeNode) &&
						nullToEmpty(parent2child.get(node)).keySet().stream()
								.allMatch(child -> child instanceof MergeNode))
				.collect(toSet());
	}

	public List<D> findParent(K parent, K child) {
		if (child.equals(parent)) return emptyList();
		Set<K> visited = new HashSet<>();
		PriorityQueue<K> queue = new PriorityQueue<>(this::compareNodes);
		queue.add(child);
		Map<K, List<D>> result = new HashMap<>();
		result.put(child, emptyList());
		while (!queue.isEmpty()) {
			K node = queue.poll();
			List<D> node2child = result.remove(node);
			if (!visited.add(node)) continue;
			assert node2child != null;
			Map<K, List<D>> nodeParents = nullToEmpty(getParents(node));
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

	public Set<K> findRoots(K node) {
		Set<K> result = new HashSet<>();
		Set<K> visited = new HashSet<>();
		ArrayList<K> queue = new ArrayList<>(singletonList(node));
		while (!queue.isEmpty()) {
			K node1 = queue.remove(queue.size() - 1);
			if (!visited.add(node1)) continue;
			Map<K, ? extends List<?>> parents = getParents(node1);
			if (parents == null) {
				result.add(node1);
			} else {
				queue.addAll(parents.keySet());
			}
		}
		return result;
	}

	public Set<K> excludeParents(Set<K> nodes) {
		Set<K> result = new LinkedHashSet<>(nodes);
		if (result.size() <= 1) return result;
		Set<K> visited = new HashSet<>();
		ArrayList<K> queue = new ArrayList<>(nodes);
		while (!queue.isEmpty()) {
			K node = queue.remove(queue.size() - 1);
			if (!visited.add(node)) continue;
			for (K parent : nullToEmpty(getParents(node)).keySet()) {
				result.remove(parent);
				if (!visited.contains(parent)) {
					queue.add(parent);
				}
			}
		}
		return result;
	}

	public Map<K, List<D>> merge(Set<K> nodes) throws OTException {
		checkArgument(nodes.size() >= 2);
		K mergeNode = doMerge(excludeParents(nodes));
		assert mergeNode != null;
		PriorityQueue<K> queue = new PriorityQueue<>(this::compareNodes);
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
			Map<K, List<D>> parentsMap = nullToEmpty(getParents(node));
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
	private K doMerge(Set<K> nodes) throws OTException {
		if (nodes.size() == 1) return first(nodes);

		K pivotNode = nodes.stream().min(comparingInt((K node) -> findRoots(node).size())).get();

		Map<K, List<D>> pivotNodeParents = getParents(pivotNode);
		Set<K> recursiveMergeNodes = union(pivotNodeParents.keySet(), difference(nodes, singleton(pivotNode)));
		K mergeNode = doMerge(excludeParents(recursiveMergeNodes));
		K parentNode = first(pivotNodeParents.keySet());
		List<D> parentToPivotNode = pivotNodeParents.get(parentNode);
		List<D> parentToMergeNode = findParent(parentNode, mergeNode);

		if (pivotNodeParents.size() > 1) {
			K resultNode = (K) new MergeNode(mergeId.incrementAndGet());
			addEdge(mergeNode, resultNode, emptyList());
			addEdge(pivotNode, resultNode,
					otSystem.squash(concat(otSystem.invert(parentToPivotNode), parentToMergeNode)));
			return resultNode;
		}

		if (pivotNodeParents.size() == 1) {
			TransformResult<D> transformed = otSystem.transform(parentToPivotNode, parentToMergeNode);
			K resultNode = (K) new MergeNode(mergeId.incrementAndGet());
			addEdge(mergeNode, resultNode, transformed.right);
			addEdge(pivotNode, resultNode, transformed.left);
			return resultNode;
		}

		throw new OTException("Graph cannot be merged");
	}

	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	public String toGraphViz() {
		StringBuilder sb = new StringBuilder();
		sb.append("digraph {\n");
		for (K child : child2parent.keySet()) {
			Map<K, List<D>> parent2diffs = child2parent.get(child);
			String color = (parent2diffs.size() == 1) ? "color=blue; " : "";
			for (K parent : parent2diffs.keySet()) {
				List<D> diffs = parent2diffs.get(parent);
				sb.append("\t" + nodeToGraphViz(child) + " -> " + nodeToGraphViz(parent) +
						" [ dir=\"back\"; " + color + "label=\"" + diffsToGraphViz(diffs) + "\"];\n");
			}
		}
		sb.append("\t{ rank=same; " +
				getOriginalTips().stream().map(OTLoadedGraph::nodeToGraphViz).collect(joining(" ")) +
				" }\n");
		sb.append("\t{ rank=same; " +
				getRoots().stream().map(OTLoadedGraph::nodeToGraphViz).collect(joining(" ")) +
				" }\n");
		sb.append("}\n");
		return sb.toString();
	}

	private static <K> String nodeToGraphViz(K node) {
		return "\"" + node + "\"";
	}

	private static <D> String diffsToGraphViz(List<D> diffs) {
		return diffs.isEmpty() ? "∅" : diffs.size() == 1 ? diffs.get(0).toString() : diffs.toString();
	}

	@Override
	public String toString() {
		return "{nodes=" + union(child2parent.keySet(), parent2child.keySet()) +
				", edges:" + parent2child.values().stream().mapToInt(Map::size).sum() + '}';
	}

}
