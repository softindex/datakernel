package io.datakernel.ot;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static io.datakernel.util.CollectionUtils.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

final class OTLoadedGraph<K, D> {
	static final AtomicLong MERGE_ID = new AtomicLong();

	static final class MergeNode {
		final long n = MERGE_ID.incrementAndGet();

		@Override
		public String toString() {
			return "@" + n;
		}
	}

	static <K> int compareNodes(K node1, K node2, Comparator<K> keyComparator) {
		if (node1 instanceof MergeNode) {
			if (node2 instanceof MergeNode) {
				return Long.compare(((MergeNode) node1).n, ((MergeNode) node2).n);
			} else {
				return +1;
			}
		} else {
			if (node2 instanceof MergeNode) {
				return -1;
			} else {
				return keyComparator.compare(node1, node2);
			}
		}
	}

	private final Map<K, Map<K, List<D>>> child2parent = new HashMap<>();
	private final Map<K, Map<K, List<D>>> parent2child = new HashMap<>();

	void add(K parent, K child, List<D> diff) {
		child2parent.computeIfAbsent(child, $ -> new HashMap<>()).put(parent, diff);
		parent2child.computeIfAbsent(parent, $ -> new HashMap<>()).put(child, diff);
	}

	Map<K, List<D>> getParents(K child) {
		return child2parent.get(child);
	}

	Set<K> getRoots() {
		return difference(parent2child.keySet(), child2parent.keySet());
	}

	Set<K> getTips() {
		return difference(child2parent.keySet(), parent2child.keySet());
	}

	Set<K> getOriginalTips() {
		return child2parent.keySet().stream()
				.filter(node -> !(node instanceof MergeNode) &&
						nullToEmpty(parent2child.get(node)).keySet().stream()
								.allMatch(child -> child instanceof MergeNode))
				.collect(toSet());
	}

	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	String toGraphViz() {
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
