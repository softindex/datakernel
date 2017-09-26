package io.datakernel.ot;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stages;

import java.util.*;
import java.util.concurrent.CompletionStage;

import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Collections.*;

public class OTUtils {
	private OTUtils() {
	}

	public static final class FindResult<K, D> {
		@Nullable
		private final OTCommit<K, D> parentCommit;
		@Nullable
		private final K child;
		@Nullable
		private final List<D> parentToChild;

		private FindResult(OTCommit<K, D> parentCommit, K child, List<D> parentToChild) {
			this.child = child;
			this.parentCommit = parentCommit;
			this.parentToChild = parentToChild;
		}

		public static <K, D> FindResult<K, D> of(OTCommit<K, D> parent, K child, List<D> pathToParent) {
			return new FindResult<>(parent, child, pathToParent);
		}

		public static <K, D> FindResult<K, D> notFound() {
			return new FindResult<>(null, null, null);
		}

		boolean isFound() {
			return parentCommit != null;
		}

		public OTCommit<K, D> getParentCommit() {
			return checkNotNull(parentCommit);
		}

		public K getChild() {
			return checkNotNull(child);
		}

		public List<D> getParentToChild() {
			return checkNotNull(parentToChild);
		}

		@Override
		public String toString() {
			return "FindResult{" +
					"parentCommit=" + parentCommit +
					", child=" + child +
					", parentToChild=" + parentToChild +
					'}';
		}
	}

	private static final class Entry<K, D> {
		final K parent;
		final K child;
		final List<D> parentToChild;

		private Entry(K parent, K child, List<D> parentToChild) {
			this.parent = parent;
			this.child = child;
			this.parentToChild = parentToChild;
		}

		@Override
		public String toString() {
			return "Entry{" +
					"parent=" + parent +
					", child=" + child +
					", parentToChild=" + parentToChild +
					'}';
		}
	}

	public static <K, D> CompletionStage<FindResult<K, D>> findCheckpoint(OTRemote<K, D> source, Comparator<K> keyComparator) {
		return source.getHeads().thenCompose(heads ->
				findCheckpoint(source, keyComparator, heads, null).thenCompose(result -> result.isFound() ?
						Stages.of(result) :
						Stages.ofException(new IllegalStateException("Could not find snapshot"))));
	}

	public static <K, D> CompletionStage<FindResult<K, D>> findCheckpoint(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                      Set<K> startNodes, @Nullable K lastNode) {
		return findParent(source, keyComparator,
				startNodes, lastNode,
				OTCommit::isCheckpoint);
	}

	public static <K, D> CompletionStage<FindResult<K, D>> findParent(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                  K startNode, @Nullable K lastNode,
	                                                                  Predicate<OTCommit<K, D>> matchPredicate) {
		return findParent(source, keyComparator, singleton(startNode), lastNode, matchPredicate);
	}

	public static <K, D> CompletionStage<FindResult<K, D>> findParent(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                  Set<K> startNodes, @Nullable K lastNode,
	                                                                  Predicate<OTCommit<K, D>> matchPredicate) {
		return findParent(source, keyComparator,
				startNodes,
				lastNode == null ?
						Predicates.alwaysTrue() :
						(Predicate<K>) key -> keyComparator.compare(key, lastNode) >= 0,
				matchPredicate);
	}

	public static <K, D> CompletionStage<FindResult<K, D>> findParent(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                  Set<K> startNodes, Predicate<K> loadPredicate,
	                                                                  Predicate<OTCommit<K, D>> matchPredicate) {
		PriorityQueue<Entry<K, D>> queue = new PriorityQueue<>(11,
				(o1, o2) -> keyComparator.compare(o2.parent, o1.parent));
		for (K startNode : startNodes) {
			queue.add(new Entry<>(startNode, startNode, emptyList()));
		}
		return findParent(source, queue, new HashSet<>(), loadPredicate, matchPredicate);
	}

	private static <K, D> CompletionStage<FindResult<K, D>> findParent(OTRemote<K, D> source,
	                                                                   PriorityQueue<Entry<K, D>> queue, Set<K> visited,
	                                                                   Predicate<K> loadPredicate,
	                                                                   Predicate<OTCommit<K, D>> matchPredicate) {
		while (!queue.isEmpty()) {
			Entry<K, D> nodeWithPath = queue.poll();
			K node = nodeWithPath.parent;
			if (!visited.add(node))
				continue;
			return source.loadCommit(node)
					.thenCompose(commit -> {
						if (matchPredicate.apply(commit)) {
							List<D> path = new ArrayList<>();
							path.addAll(nodeWithPath.parentToChild);
							return Stages.of(FindResult.of(commit, nodeWithPath.child, path));
						}
						for (Map.Entry<K, List<D>> parentEntry : commit.getParents().entrySet()) {
							K parent = parentEntry.getKey();
							if (parentEntry.getValue() == null)
								continue;
							if (loadPredicate.apply(parent)) {
								List<D> parentDiffs = new ArrayList<>();
								parentDiffs.addAll(parentEntry.getValue());
								parentDiffs.addAll(nodeWithPath.parentToChild);
								queue.add(new Entry<>(parent, nodeWithPath.child, parentDiffs));
							}
						}
						return findParent(source, queue, visited, loadPredicate, matchPredicate);
					});
		}
		return Stages.of(FindResult.notFound());
	}

	public static <K1, K2, V> Map<K2, V> ensureMapValue(Map<K1, Map<K2, V>> map, K1 key) {
		return map.computeIfAbsent(key, k -> new HashMap<>());
	}

	public static <K, V> Set<V> ensureSetValue(Map<K, Set<V>> map, K key) {
		return map.computeIfAbsent(key, k -> new HashSet<>());
	}

	public static <K, V> List<V> ensureListValue(Map<K, List<V>> map, K key) {
		return map.computeIfAbsent(key, k -> new ArrayList<>());
	}

	private static <K, D> CompletionStage<List<OTCommit<K, D>>> loadEdge(OTRemote<K, D> source, OTSystem<D> otSystem,
	                                                                     K node) {
		return source.loadCommit(node).thenCompose(commit -> {
			if (commit.isRoot() || commit.isMerge()) {
				List<OTCommit<K, D>> edge = new ArrayList<>();
				edge.add(commit);
				return Stages.of(edge);
			}
			assert commit.getParents().size() == 1;
			K parentId = commit.getParents().keySet().iterator().next();
			return loadEdge(source, otSystem, parentId).thenApply(edge -> {
				edge.add(commit);
				return edge;
			});
		});
	}

	public static <K, D> CompletionStage<Map<K, List<D>>> merge(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                            Set<K> nodes) {
		return doMerge(otSystem, source, keyComparator, nodes, new HashSet<>(), null);
	}

	public static <K, D> List<D> diff(List<OTCommit<K, D>> path) {
		List<D> result = new ArrayList<>();
		K prev = null;
		for (OTCommit<K, D> commit : path) {
			if (prev != null) {
				result.addAll(commit.getParents().get(prev));
			}
			prev = commit.getId();
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public static <K, D> CompletionStage<Map<K, List<D>>> doMerge(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                              Set<K> nodes, Set<K> visitedNodes, K rootNode) {
		if (nodes.size() == 0) {
			return Stages.of(emptyMap());
		}

		if (nodes.size() == 1) {
			Map<K, List<D>> result = new HashMap<>();
			K node = nodes.iterator().next();
			result.put(node, emptyList());
			visitedNodes.add(node);
			return Stages.of(result);
		}

		K lastNode = null;
		for (K node : nodes) {
			if (rootNode != null && rootNode.equals(node)) {
				continue;
			}
			if (lastNode == null || keyComparator.compare(node, lastNode) > 0) {
				lastNode = node;
			}
		}
		Set<K> earlierNodes = new HashSet<>(nodes);
		earlierNodes.remove(lastNode);

		final K finalLastNode = lastNode;
		return loadEdge(source, otSystem, lastNode).thenCompose((List<OTCommit<K, D>> edge) -> {
			if (rootNode == null && edge.get(0).isRoot()) {
				return doMerge(otSystem, source, keyComparator, nodes, visitedNodes, edge.get(0).getId());
			}

			if (edge.size() != 1) {
				OTCommit<K, D> base = edge.get(0);
				Set<K> recursiveMergeNodes = new HashSet<>(earlierNodes);
				recursiveMergeNodes.add(base.getId());

				return doMerge(otSystem, source, keyComparator, recursiveMergeNodes, visitedNodes, rootNode).thenApply((Map<K, List<D>> mergeResult) -> {
					int surfaceNodeIdx = 0;
					for (int i = 0; i < edge.size(); i++) {
						if (visitedNodes.contains(edge.get(i).getId())) {
							surfaceNodeIdx = i;
						} else {
							break;
						}
					}

					List<OTCommit<K, D>> inner = edge.subList(0, surfaceNodeIdx + 1);
					List<OTCommit<K, D>> outer = edge.subList(surfaceNodeIdx, edge.size());

					List<D> surfaceToMerge = new ArrayList<>();
					surfaceToMerge.addAll(otSystem.invert(diff(inner)));
					surfaceToMerge.addAll(mergeResult.get(base.getId()));

					List<D> surfaceToLast = diff(outer);

					List<D> squash = otSystem.squash(surfaceToMerge);
					TransformResult<D> transformed = otSystem.transform(squash, otSystem.squash(surfaceToLast));

					Map<K, List<D>> result = new HashMap<>();
					result.put(finalLastNode, transformed.right);
					for (K node : earlierNodes) {
						List<D> list = new ArrayList<>();
						list.addAll(mergeResult.get(node));
						list.addAll(transformed.left);
						result.put(node, otSystem.squash(list));
					}

					edge.stream().map(OTCommit::getId).forEach(visitedNodes::add);

					return result;
				});
			} else {
				OTCommit<K, D> last = edge.get(0);
				Set<K> recursiveMergeNodes = new HashSet<>(earlierNodes);
				recursiveMergeNodes.addAll(last.getParents().keySet());

				return doMerge(otSystem, source, keyComparator, recursiveMergeNodes, visitedNodes, rootNode).thenApply((Map<K, List<D>> mergeResult) -> {
					K parentNode = null;
					for (Map.Entry<K, List<D>> entry : last.getParents().entrySet()) {
						if (entry.getValue() == null)
							continue;
						parentNode = entry.getKey();
						break;
					}

					List<D> baseToMerge = new ArrayList<>();
					baseToMerge.addAll(otSystem.invert(last.getParents().get(parentNode)));
					baseToMerge.addAll(mergeResult.get(parentNode));

					Map<K, List<D>> result = new HashMap<>();
					result.put(finalLastNode, otSystem.squash(baseToMerge));
					for (K node : earlierNodes) {
						result.put(node, mergeResult.get(node));
					}

					edge.stream().map(OTCommit::getId).forEach(visitedNodes::add);

					return result;
				});

			}
		});
	}

	public static <K, D> CompletionStage<K> mergeHeadsAndPush(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator) {
		return source.getHeads().thenCompose(heads ->
				merge(otSystem, source, keyComparator, heads).thenCompose(merge ->
						source.createId().thenCompose(mergeCommitId ->
								source.push(singletonList(OTCommit.ofMerge(mergeCommitId, merge)))
										.thenApply($ -> mergeCommitId))));
	}

	private static <K, D> CompletionStage<K> doMakeCheckpoint(OTSystem<D> otSystem, OTRemote<K, D> source,
	                                                          FindResult<K, D> result) {
		if (result.isFound()) {
			List<D> diffs = new ArrayList<>();
			diffs.addAll(result.getParentCommit().getCheckpoint());
			diffs.addAll(result.getParentToChild());
			List<D> checkpoint = otSystem.squash(diffs);

			return source.createId().thenCompose(checkpointId -> {
				OTCommit<K, D> commit = OTCommit.ofCheckpoint(checkpointId, result.getChild(), checkpoint);
				return source.push(singletonList(commit))
						.thenApply($ -> checkpointId);
			});
		} else {
			return Stages.ofException(new IllegalArgumentException("No checkpoint found for HEAD(s)"));
		}
	}

	public static <K, D> CompletionStage<K> makeCheckpointForHeads(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator) {
		return findCheckpoint(source, keyComparator).thenCompose(result ->
				doMakeCheckpoint(otSystem, source, result));
	}

	public static <K, D> CompletionStage<K> makeCheckpointForNode(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                              K node) {
		return findCheckpoint(source, keyComparator, singleton(node), null).thenCompose(result ->
				doMakeCheckpoint(otSystem, source, result));
	}

}
