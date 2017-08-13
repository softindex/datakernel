package io.datakernel.ot;

import io.datakernel.util.Preconditions;

import java.util.*;

public class OTGraph<K, D> {
	public final OTSystem<D> otSystem;
	public final Comparator<K> keyComparator;
	public final Map<K, Map<K, List<D>>> forward = new HashMap<>();
	public final Map<K, Map<K, List<D>>> backward = new HashMap<>();

	public OTGraph(OTSystem<D> otSystem, Comparator<K> keyComparator) {
		this.otSystem = otSystem;
		this.keyComparator = keyComparator;
	}

	static final class Path<K, D> {
		final K from;
		final K to;
		final List<D> ops;

		Path(K from, K to, List<D> ops) {
			this.from = from;
			this.to = to;
			this.ops = ops;
		}

		@Override
		public String toString() {
			return "{" + from + "->" + to + ": " + ops + '}';
		}
	}

	static final class PathSet<K, D> {
		final Map<K, Set<Path<K, D>>> from = new HashMap<>();
		final Map<K, Set<Path<K, D>>> to = new HashMap<>();
		final Set<Path<K, D>> paths = new HashSet<>();

		private Set<Path<K, D>> ensureFrom(K key) {
			Set<Path<K, D>> paths = from.get(key);
			if (paths == null) {
				paths = new HashSet<>();
				from.put(key, paths);
			}
			return paths;
		}

		private Set<Path<K, D>> ensureTo(K key) {
			Set<Path<K, D>> paths = to.get(key);
			if (paths == null) {
				paths = new HashSet<>();
				to.put(key, paths);
			}
			return paths;
		}

		public void add(Path<K, D> path) {
			Preconditions.check(!paths.contains(path));
			paths.add(path);
			ensureFrom(path.from).add(path);
			ensureTo(path.to).add(path);
		}

		@Override
		public String toString() {
			return paths.toString();
		}
	}

	public void add(K from, K to, List<? extends D> ops) {
		Map<K, List<D>> forwardMap = forward.get(from);
		if (forwardMap == null) {
			forwardMap = new HashMap<>();
			forward.put(from, forwardMap);
		}
		Preconditions.check(!forwardMap.containsKey(to));
		forwardMap.put(to, (List<D>) ops);

		Map<K, List<D>> backwardMap = backward.get(to);
		if (backwardMap == null) {
			backwardMap = new HashMap<>();
			backward.put(to, backwardMap);
		}
		Preconditions.check(!backwardMap.containsKey(from));
		backwardMap.put(from, (List<D>) ops);
	}

	public void add(K node, Map<K, List<? extends D>> parents) {
		for (K parent : parents.keySet()) {
			List<? extends D> ops = parents.get(parent);
			add(parent, node, ops);
		}
	}

	public Path<K, D> getBasePath(K key) {
		K from = key;
		List<D> ops = new ArrayList<>();
		while (true) {
			Map<K, List<D>> backwardMap = backward.get(key);
			if (backwardMap == null || backwardMap.size() != 1)
				break;
			Map.Entry<K, List<D>> entry = backwardMap.entrySet().iterator().next();
			key = entry.getKey();
			ops.addAll(0, entry.getValue());
		}
		return new Path<>(key, from, ops);
	}

	public Map<K, List<D>> merge(Set<K> nodes) {
		return merge(nodes, new HashSet<K>());
	}

	@SuppressWarnings("unchecked")
	public Map<K, List<D>> merge(Set<K> nodes, Set<K> visitedNodes) {
		if (nodes.size() == 0) {
			return Collections.emptyMap();
		}
		if (nodes.size() == 1) {
			Map<K, List<D>> result = new HashMap<>();
			K node = nodes.iterator().next();
			result.put(node, Collections.<D>emptyList());
			visitedNodes.add(node);
			return result;
		}

		List<K> otherNodes = new ArrayList<>(nodes);
		Collections.sort(otherNodes, keyComparator);
		K childNode = otherNodes.remove(otherNodes.size() - 1);
		Map<K, List<D>> parents = backward.get(childNode);
		if (parents == null) {
			otherNodes.add(childNode);
			childNode = otherNodes.remove(otherNodes.size() - 2);
			parents = backward.get(childNode);
		}
		Set<K> allNodes = new HashSet<>(otherNodes);
		allNodes.addAll(parents.keySet());

		Map<K, List<D>> mergeResult = merge(allNodes, visitedNodes);

		K parent = null;
		for (Map.Entry<K, List<D>> entry : parents.entrySet()) {
			if (entry.getValue() == null)
				continue;
			parent = entry.getKey();
			break;
		}

		Map<K, List<D>> result = new HashMap<>();

		if (parents.size() == 1 && !visitedNodes.contains(childNode)) {
			DiffPair<D> transformed = otSystem.transform(DiffPair.of(parents.get(parent), mergeResult.get(parent)));

			for (K node : otherNodes) {
				List<D> list = new ArrayList<>(mergeResult.get(node));
				list.addAll(transformed.right);
				result.put(node, list);
			}

			result.put(childNode, transformed.left);
		} else {
			for (K node : otherNodes) {
				result.put(node, mergeResult.get(node));
			}

			List<D> childPath = new ArrayList<>();
			childPath.addAll(otSystem.invert(parents.get(parent)));
			childPath.addAll(mergeResult.get(parent));
			result.put(childNode, childPath);
		}

		visitedNodes.add(childNode);

		return result;
	}

}
