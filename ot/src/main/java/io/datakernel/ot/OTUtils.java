package io.datakernel.ot;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.Stages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.util.CollectionUtils.$;
import static io.datakernel.util.CollectionUtils.concat;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toMap;

public class OTUtils {
	private static final Logger logger = LoggerFactory.getLogger(OTUtils.class);

	private OTUtils() {
	}

	public static final class FindResult<K, D, A> {
		@Nullable
		private final OTCommit<K, D> parentCommit;
		@Nullable
		private final K child;
		@Nullable
		private final A accumulator;

		private FindResult(OTCommit<K, D> parentCommit, K child, A accumulator) {
			this.child = child;
			this.parentCommit = parentCommit;
			this.accumulator = accumulator;
		}

		public static <K, D, A> FindResult<K, D, A> of(OTCommit<K, D> parent, K child, A accumulator) {
			return new FindResult<>(parent, child, accumulator);
		}

		public static <K, D, A> FindResult<K, D, A> notFound() {
			return new FindResult<>(null, null, null);
		}

		public boolean isFound() {
			return parentCommit != null;
		}

		public OTCommit<K, D> getParentCommit() {
			return checkNotNull(parentCommit);
		}

		public K getChild() {
			return checkNotNull(child);
		}

		public K getParent() {
			return parentCommit.getId();
		}

		public A getAccumulator() {
			return checkNotNull(accumulator);
		}

		@Override
		public String toString() {
			return "FindResult{" +
					"parentCommit=" + parentCommit +
					", child=" + child +
					", accumulator=" + accumulator +
					'}';
		}
	}

	private static final class Entry<K, A> {
		final K parent;
		final K child;
		final A accumulator;

		private Entry(K parent, K child, A accumulator) {
			this.parent = parent;
			this.child = child;
			this.accumulator = accumulator;
		}

		@Override
		public String toString() {
			return "Entry{" +
					"parent=" + parent +
					", child=" + child +
					", accumulator=" + accumulator +
					'}';
		}
	}

	public static <K, D> CompletionStage<FindResult<K, D, List<D>>> findParentCommits(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                                  K startNode, @Nullable K lastNode,
	                                                                                  AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		return findParent(source, keyComparator, startNode, new ArrayList<>(), lastNode, matchPredicate, (ds, ds2) -> concat(ds2, ds));
	}

	public static <K, D, A> CompletionStage<FindResult<K, D, A>> findParent(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                        K startNode, A accumulator, @Nullable K lastNode,
	                                                                        AsyncPredicate<OTCommit<K, D>> matchPredicate,
	                                                                        DiffReducer<A, D> reducer) {
		return findParent(source, keyComparator, singletonMap(startNode, accumulator), lastNode, matchPredicate, reducer);
	}

	public static <K, D> CompletionStage<FindResult<K, D, List<D>>> findParentCommits(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                                  Set<K> startNodes, @Nullable K lastNode,
	                                                                                  AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		final Map<K, List<D>> startMap = startNodes.stream().collect(toMap(Function.identity(), $::newArrayList));
		return findParent(source, keyComparator, startMap, lastNode, matchPredicate, (ds, ds2) -> concat(ds2, ds));
	}

	public static <K, D, A> CompletionStage<FindResult<K, D, A>> findParent(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                        Map<K, A> startNodes, @Nullable K lastNode,
	                                                                        AsyncPredicate<OTCommit<K, D>> matchPredicate,
	                                                                        DiffReducer<A, D> reducer) {
		final Predicate<K> predicate = lastNode == null
				? k -> true
				: key -> keyComparator.compare(key, lastNode) >= 0;

		return findParent(source, keyComparator, startNodes, predicate, matchPredicate, reducer);
	}

	public static <K, D> CompletionStage<FindResult<K, D, List<D>>> findParentCommits(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                                  Set<K> startNodes, Predicate<K> loadPredicate,
	                                                                                  AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		final Map<K, List<D>> startMap = startNodes.stream().collect(toMap(Function.identity(), k -> new ArrayList<>()));
		return findParent(source, keyComparator, startMap, loadPredicate, matchPredicate, (ds, ds2) -> concat(ds2, ds));
	}

	public static <K, D, A> CompletionStage<FindResult<K, D, A>> findParent(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                        Map<K, A> startNodes, Predicate<K> loadPredicate,
	                                                                        AsyncPredicate<OTCommit<K, D>> matchPredicate,
	                                                                        DiffReducer<A, D> reducer) {
		final PriorityQueue<Entry<K, A>> queue = new PriorityQueue<>(11,
				(o1, o2) -> keyComparator.compare(o2.parent, o1.parent));

		startNodes.forEach((startNode, acc) -> queue.add(new Entry<>(startNode, startNode, acc)));

		return findParent(source, queue, new HashSet<>(), loadPredicate, matchPredicate, reducer);
	}

	private static <K, D, A> CompletionStage<FindResult<K, D, A>> findParent(OTRemote<K, D> source,
	                                                                         PriorityQueue<Entry<K, A>> queue,
	                                                                         Set<K> visited,
	                                                                         Predicate<K> loadPredicate,
	                                                                         AsyncPredicate<OTCommit<K, D>> matcher,
	                                                                         DiffReducer<A, D> reducer) {
		while (!queue.isEmpty()) {
			final Entry<K, A> nodeWithPath = queue.poll();
			final K node = nodeWithPath.parent;
			final A nodeAccumulator = nodeWithPath.accumulator;
			final K child = nodeWithPath.child;
			if (!visited.add(node)) continue;

			return source.loadCommit(node).thenComposeAsync(commit ->
					matcher.apply(commit).thenComposeAsync(match -> {
						if (match) return Stages.of(FindResult.of(commit, child, nodeAccumulator));

						for (Map.Entry<K, List<D>> parentEntry : commit.getParents().entrySet()) {
							if (parentEntry.getValue() == null) continue;

							final K parent = parentEntry.getKey();
							if (loadPredicate.test(parent)) {
								A accumulator = reducer.apply(nodeAccumulator, parentEntry.getValue());
								queue.add(new Entry<>(parent, child, accumulator));
							}
						}

						return findParent(source, queue, visited, loadPredicate, matcher, reducer);
					}));
		}

		return Stages.of(FindResult.notFound());
	}

	public static <K, D> CompletionStage<K> mergeHeadsAndPush(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator) {
		return source.getHeads().thenCompose(heads -> mergeHeadsAndPush(otSystem, source, keyComparator, heads));
	}

	public static <K, D> CompletionStage<K> tryMergeHeadsAndPush(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator) {
		return source.getHeads().thenCompose(heads -> heads.size() != 1 ?
				mergeHeadsAndPush(otSystem, source, keyComparator, heads) :
				Stages.of(heads.iterator().next()));
	}

	private static <K, D> CompletionStage<K> mergeHeadsAndPush(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator, Set<K> heads) {
		return OTMergeAlgorithm.loadAndMerge(otSystem, source, keyComparator, heads).thenCompose(merge ->
				source.createId().thenCompose(mergeCommitId ->
						source.push(singletonList(OTCommit.ofMerge(mergeCommitId, merge)))
								.thenApply($ -> mergeCommitId)));
	}

	public static <K, D> CompletionStage<FindResult<K, D, List<D>>> findSnapshotOrRoot(OTRemote<K, D> source, Comparator<K> keyComparator, Set<K> startNode, @Nullable K lastNode) {
		return findParentCommits(source, keyComparator, startNode, lastNode, commit -> {
			if (commit.isRoot()) return Stages.of(true);
			return source.isSnapshot(commit.getId());
		});
	}

	public static <K, D> CompletionStage<FindResult<K, D, List<D>>> findChildPath(OTRemote<K, D> source, Comparator<K> keyComparator, K startNode, K headNode) {
		return findParentCommits(source, keyComparator, headNode, startNode, commit -> Stages.of(startNode.equals(commit.getId())));
	}

	public static <K, D> CompletionStage<Set<K>> findParentCandidates(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                  Set<K> startNodes, Predicate<K> loadPredicate,
	                                                                  Predicate<OTCommit<K, D>> matchPredicate) {
		final PriorityQueue<Entry<K, List<D>>> queue = new PriorityQueue<>((o1, o2) ->
				keyComparator.compare(o2.parent, o1.parent));

		for (K startNode : startNodes) {
			queue.add(new Entry<>(startNode, startNode, emptyList()));
		}

		return findParentCandidates(source, new HashSet<>(), new HashSet<>(), queue, loadPredicate, matchPredicate);
	}

	private static <K, D> CompletionStage<Set<K>> findParentCandidates(OTRemote<K, D> source, Set<K> visited,
	                                                                   Set<K> candidates, PriorityQueue<Entry<K, List<D>>> queue,
	                                                                   Predicate<K> loadPredicate,
	                                                                   Predicate<OTCommit<K, D>> matchPredicate) {
		while (!queue.isEmpty()) {
			final Entry<K, List<D>> nodeWithPath = queue.poll();
			K node = nodeWithPath.parent;
			if (!visited.add(node)) continue;

			return source.loadCommit(node).thenComposeAsync(commit -> {
				if (matchPredicate.test(commit)) {
					candidates.add(node);
				} else {
					for (Map.Entry<K, List<D>> parentEntry : commit.getParents().entrySet()) {
						final K parent = parentEntry.getKey();
						if (parentEntry.getValue() == null) continue;

						if (loadPredicate.test(parent)) {
							List<D> parentDiffs = new ArrayList<>();
							parentDiffs.addAll(parentEntry.getValue());
							parentDiffs.addAll(nodeWithPath.accumulator);
							queue.add(new Entry<>(parent, nodeWithPath.child, parentDiffs));
						}
					}
				}
				return findParentCandidates(source, visited, candidates, queue, loadPredicate, matchPredicate);
			});
		}
		return Stages.of(candidates);
	}

	public static <K, D> CompletionStage<Set<K>> findParentCandidatesSurface(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                         Set<K> startNodes, AsyncPredicate<Set<K>> matchPredicate) {
		final PriorityQueue<K> queue = new PriorityQueue<>((o1, o2) -> keyComparator.compare(o2, o1));
		final Set<K> surface = new HashSet<>();

		queue.addAll(startNodes);
		surface.addAll(startNodes);

		return findParentCandidatesSurface(source, new HashSet<>(), surface, queue, matchPredicate);
	}

	private static <K, D> CompletionStage<Set<K>> findParentCandidatesSurface(OTRemote<K, D> source, Set<K> visited,
	                                                                          Set<K> surface, PriorityQueue<K> queue,
	                                                                          AsyncPredicate<Set<K>> matchPredicate) {
		while (!queue.isEmpty()) {
			final K node = queue.poll();
			surface.remove(node);
			if (!visited.add(node)) continue;

			return source.loadCommit(node).thenComposeAsync(commit -> {
				surface.addAll(commit.getParentIds());
				queue.addAll(commit.getParentIds());

				return matchPredicate.apply(surface).thenCompose(result -> result ? Stages.of(surface)
						: findParentCandidatesSurface(source, visited, surface, queue, matchPredicate));
			});
		}
		return Stages.of(surface);
	}

	public static <K, D> CompletionStage<Optional<K>> findCommonParentIncludingCandidates(OTRemote<K, D> source,
	                                                                                      Comparator<K> keyComparator,
	                                                                                      Set<K> parentCandidates) {
		final Predicate<Map<K, Set<K>>> matcher = paths -> paths.values().stream()
				.anyMatch(v -> v.size() == parentCandidates.size());

		return findCommonParents(source, keyComparator, parentCandidates, matcher)
				.thenApply(paths -> paths.entrySet().stream()
						.filter(e -> e.getValue().containsAll(parentCandidates))
						.map(Map.Entry::getKey)
						.findAny());
	}

	public static <K, D> CompletionStage<Set<K>> findCommonParentsIncludingCandidates(OTRemote<K, D> source,
	                                                                                  Comparator<K> keyComparator,
	                                                                                  Set<K> parentCandidates) {
		final Predicate<Map<K, Set<K>>> matcher = paths -> paths.values().stream()
				.noneMatch(v -> v.size() != parentCandidates.size());

		return findCommonParents(source, keyComparator, parentCandidates, matcher).thenApply(Map::keySet);
	}

	private static <K, D> CompletionStage<Map<K, Set<K>>> findCommonParents(OTRemote<K, D> source,
	                                                                        Comparator<K> keyComparator,
	                                                                        Set<K> parentCandidates,
	                                                                        Predicate<Map<K, Set<K>>> matcher) {
		final Map<K, Set<K>> childrenMap = new HashMap<>();
		final PriorityQueue<K> queue = new PriorityQueue<>((o1, o2) -> keyComparator.compare(o2, o1));

		queue.addAll(parentCandidates);
		parentCandidates.forEach(node -> childrenMap.put(node, new HashSet<>(singleton(node))));

		return findCommonParents(source, parentCandidates, queue, childrenMap, matcher);
	}

	private static <K, D> CompletionStage<Map<K, Set<K>>> findCommonParents(OTRemote<K, D> source,
	                                                                        Set<K> parentCandidates,
	                                                                        PriorityQueue<K> queue,
	                                                                        Map<K, Set<K>> childrenMap,
	                                                                        Predicate<Map<K, Set<K>>> matcher) {
		logger.debug("search root nodes: queue {}, childrenMap {}", queue, childrenMap);

		if (matcher.test(childrenMap)) return Stages.of(childrenMap);
		if (queue.isEmpty()) return Stages.of(Collections.emptyMap());

		final K node = queue.poll();
		final Set<K> nodeChildren = childrenMap.remove(node);
		return source.loadCommit(node).thenComposeAsync(commit -> {
			final Set<K> parents = commit.getParentIds();
			logger.debug("Commit: {}, parents: {}", node, parents);
			for (K parent : parents) {
				if (!childrenMap.containsKey(parent)) queue.add(parent);
				final Set<K> children = childrenMap.computeIfAbsent(parent, k -> new HashSet<>(nodeChildren.size() + 2));

				if (parentCandidates.contains(parent)) children.add(parent);
				if (parentCandidates.contains(node)) children.add(node);
				children.addAll(nodeChildren);
			}

			return findCommonParents(source, parentCandidates, queue, childrenMap, matcher);
		});
	}

	private static class ReduceEntry<K, A> {
		public final K parent;
		public final A accumulator;

		private ReduceEntry(K parent, A accumulator) {
			this.parent = parent;
			this.accumulator = accumulator;
		}
	}

	public static <K, D, A> CompletionStage<Map<K, A>> reduceEdges(OTRemote<K, D> source, Comparator<K> comparator,
	                                                               Map<K, A> heads, K commonNode,
	                                                               Predicate<K> loadPredicate,
	                                                               DiffReducer<A, D> reducer,
	                                                               BiFunction<A, A, A> reduceAccumulator) {
		final PriorityQueue<ReduceEntry<K, Map<K, A>>> queue = new PriorityQueue<>(11,
				(o1, o2) -> comparator.compare(o2.parent, o1.parent));

		heads.forEach((head, accumulator) -> {
			final Map<K, A> map = new HashMap<>();
			map.put(head, accumulator);

			queue.add(new ReduceEntry<>(head, map));
		});

		return reduceEdges(source, queue, commonNode, loadPredicate, reducer, reduceAccumulator)
				.handle((value, throwable) -> (throwable == null && value.keySet().equals(heads.keySet()))
						? Stages.of(value)
						: Stages.<Map<K, A>>ofException(new IllegalArgumentException(noPathMessage(heads, commonNode))))
				.thenCompose(Function.identity());
	}

	private static <K, A> String noPathMessage(Map<K, A> heads, K commonNode) {
		return format("No path from heads `%s` to common node: `%s`", heads.keySet(), commonNode);
	}

	private static <K, D, A> CompletionStage<Map<K, A>> reduceEdges(OTRemote<K, D> source,
	                                                                PriorityQueue<ReduceEntry<K, Map<K, A>>> queue,
	                                                                K commonNode, Predicate<K> loadPredicate,
	                                                                DiffReducer<A, D> reducer,
	                                                                BiFunction<A, A, A> reduceAccumulator) {
		if (queue.isEmpty()) return Stages.of(new HashMap<K, A>());

		final ReduceEntry<K, Map<K, A>> value = queue.poll();
		final K key = value.parent;
		final Map<K, A> accumulator = value.accumulator;

		while (!queue.isEmpty() && queue.peek().parent.equals(key)) {
			final ReduceEntry<K, Map<K, A>> poll = queue.poll();
			poll.accumulator.forEach((k, newValue) -> accumulator.merge(k, newValue, reduceAccumulator));
		}

		if (key.equals(commonNode)) {
			return reduceEdges(source, queue, commonNode, loadPredicate, reducer, reduceAccumulator)
					.thenApplyAsync(kaMap -> {
						accumulator.forEach((k, a) -> kaMap.merge(k, a, reduceAccumulator));
						return kaMap;
					});
		}

		return source.loadCommit(key)
				.thenAccept(commit -> commit.getParents().entrySet().stream()
						.filter(e -> loadPredicate.test(e.getKey()) && e.getValue() != null)
						.map(e -> new ReduceEntry<>(e.getKey(), reduce(accumulator, e.getValue(), reducer)))
						.forEach(queue::add))
				.thenComposeAsync($ -> reduceEdges(source, queue, commonNode, loadPredicate, reducer, reduceAccumulator));
	}

	private static <K, D, A> Map<K, A> reduce(Map<K, A> accumulators, List<D> diffs, DiffReducer<A, D> reducer) {
		return accumulators.entrySet().stream()
				.collect(toMap(Map.Entry::getKey, e -> reducer.apply(e.getValue(), diffs)));
	}

	public static <K, D> CompletionStage<List<D>> loadAllChanges(OTRemote<K, D> source, Comparator<K> comparator,
	                                                             OTSystem<D> otSystem, K head) {
		return findSnapshotOrRoot(source, comparator, singleton(head), null).thenCompose(result -> {
			final List<D> parentToChild = result.getAccumulator();
			final OTCommit<K, D> parentCommit = result.getParentCommit();

			// root can also be snapshot
			return source.isSnapshot(result.getParent()).thenCompose(snapshot -> {
				if (!snapshot) return Stages.of(otSystem.squash(parentToChild));

				return source.loadSnapshot(parentCommit.getId()).thenApply(diffs -> {
					final List<D> changes = new ArrayList<>(diffs.size() + parentToChild.size());
					changes.addAll(diffs);
					changes.addAll(parentToChild);
					return otSystem.squash(changes);
				});

			});
		});
	}

	public static <K, D> CompletionStage<Void> saveSnapshot(OTRemote<K, D> source, Comparator<K> comparator,
	                                                        OTSystem<D> otSystem, K head) {
		return loadAllChanges(source, comparator, otSystem, head)
				.thenCompose(ds -> source.saveSnapshot(head, ds));
	}
}
