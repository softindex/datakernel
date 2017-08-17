package io.datakernel.ot;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.datakernel.annotation.Nullable;

import java.util.*;
import java.util.concurrent.CompletionStage;

import static io.datakernel.async.SettableStage.immediateFailedStage;
import static io.datakernel.async.SettableStage.immediateStage;
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
				findCheckpoint(source, keyComparator, heads, null).thenCompose(result ->
						result.isFound() ?
								immediateStage(result) :
								immediateFailedStage(new IllegalStateException("Could not find snapshot"))));
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
							return immediateStage(FindResult.of(commit, nodeWithPath.child, path));
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
		return immediateStage(FindResult.notFound());
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

	private static <K, D> CompletionStage<OTCommit<K, D>> loadCommitForMerge(OTRemote<K, D> source, OTSystem<D> otSystem,
	                                                                         K node, Set<K> visitedNodes,
	                                                                         List<D> squashedPath, Set<K> squashedPathNodes) {
		squashedPathNodes.add(node);
		return source.loadCommit(node)
				.thenCompose(commit -> {
					if (commit.isRoot() || commit.isMerge() || visitedNodes.contains(commit.getId())) {
						return immediateStage(commit);
					}
					assert commit.getParents().size() == 1;
					Map.Entry<K, List<D>> parentEntry = commit.getParents().entrySet().iterator().next();
					K parentId = parentEntry.getKey();
					List<D> parentPath = parentEntry.getValue();
					squashedPath.addAll(0, parentPath);
					return loadCommitForMerge(source, otSystem, parentId, visitedNodes, squashedPath, squashedPathNodes);
				});
	}

	public static <K, D> CompletionStage<Map<K, List<D>>> merge(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                            Set<K> nodes) {
		return doMerge(otSystem, source, keyComparator, nodes, new HashSet<>(), null);
	}

	@SuppressWarnings("unchecked")
	public static <K, D> CompletionStage<Map<K, List<D>>> doMerge(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                              Set<K> nodes, Set<K> visitedNodes, K rootNode) {
		if (nodes.size() == 0) {
			return immediateStage(emptyMap());
		}

		if (nodes.size() == 1) {
			Map<K, List<D>> result = new HashMap<>();
			K node = nodes.iterator().next();
			result.put(node, emptyList());
			visitedNodes.add(node);
			return immediateStage(result);
		}

		K pivot = null;
		for (K node : nodes) {
			if (rootNode != null && rootNode.equals(node)) {
				continue;
			}
			if (pivot == null || keyComparator.compare(pivot, node) > 0) {
				pivot = node;
			}
		}
		Set<K> otherNodes = new HashSet<>(nodes);
		otherNodes.remove(pivot);

		List<D> squashPath = new ArrayList<>();
		Set<K> squashNodes = new HashSet<>();
		K finalPivot = pivot;
		return loadCommitForMerge(source, otSystem, pivot, visitedNodes, squashPath, squashNodes).thenCompose(commit -> {
			if (rootNode == null && commit.isRoot()) {
				return doMerge(otSystem, source, keyComparator, nodes, visitedNodes, commit.getId());
			}
			visitedNodes.addAll(squashNodes);

			Set<K> recursiveMergeNodes = new HashSet<>(otherNodes);
			recursiveMergeNodes.addAll(commit.isRoot() ? singleton(commit.getId()) : commit.getParents().keySet());

			return doMerge(otSystem, source, keyComparator, recursiveMergeNodes, visitedNodes, rootNode).thenApply(mergeResult -> {
				K parent = null;
				for (Map.Entry<K, List<D>> entry : commit.getParents().entrySet()) {
					if (entry.getValue() == null)
						continue;
					parent = entry.getKey();
					break;
				}

				if (commit.isRoot())
					parent = commit.getId();

				Map<K, List<D>> result = new HashMap<>();

				List<D> pivotPath = new ArrayList<>();
				if (!commit.isRoot())
					pivotPath.addAll(otSystem.invert(commit.getParents().get(parent)));
				pivotPath.addAll(mergeResult.get(parent));

				DiffPair<D> transformed = otSystem.transform(DiffPair.of(otSystem.squash(squashPath), otSystem.squash(pivotPath)));

				result.put(finalPivot, transformed.left);

				for (K node : otherNodes) {
					List<D> list = new ArrayList<>();
					list.addAll(mergeResult.get(node));
					list.addAll(transformed.right);
					result.put(node, list);
				}

				return result;
			});
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
			return immediateFailedStage(new IllegalArgumentException("No checkpoint found for HEAD(s)"));
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
