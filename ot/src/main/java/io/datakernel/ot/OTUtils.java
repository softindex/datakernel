package io.datakernel.ot;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.Stages;
import io.datakernel.ot.exceptions.OTException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.util.CollectionUtils.$;
import static io.datakernel.util.CollectionUtils.concat;
import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toMap;

public class OTUtils {
	private static final Logger logger = LoggerFactory.getLogger(OTUtils.class);

	private OTUtils() {
	}

	public static final class FindResult<K, A> {
		@Nullable
		private final K commit;
		@Nullable
		private final Set<K> parents;
		@Nullable
		private final K child;
		@Nullable
		private final A accumulator;

		private FindResult(K commit, K child, Set<K> parents, A accumulator) {
			this.child = child;
			this.commit = commit;
			this.parents = parents;
			this.accumulator = accumulator;
		}

		public static <K, A> FindResult<K, A> of(K commit, K child, Set<K> parents, A accumulator) {
			return new FindResult<>(commit, child, parents, accumulator);
		}

		public static <K, A> FindResult<K, A> notFound() {
			return new FindResult<>(null, null, null, null);
		}

		public boolean isFound() {
			return commit != null;
		}

		public K getCommit() {
			return checkNotNull(commit);
		}

		public K getChild() {
			return checkNotNull(child);
		}

		public Set<K> getCommitParents() {
			return checkNotNull(parents);
		}

		public A getAccumulator() {
			return checkNotNull(accumulator);
		}

		@Override
		public String toString() {
			return "FindResult{" +
					"commit=" + commit +
					", parents=" + parents +
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

	public static <K, D> CompletionStage<FindResult<K, List<D>>> findParentCommits(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                               K startNode, @Nullable K lastNode,
	                                                                               AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		return findParent(source, keyComparator, startNode, new ArrayList<>(), lastNode, matchPredicate, (ds, ds2) -> concat(ds2, ds));
	}

	public static <K, D> CompletionStage<FindResult<K, List<D>>> findParentCommits(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                               Set<K> startNodes, @Nullable K lastNode,
	                                                                               AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		Map<K, List<D>> startMap = startNodes.stream().collect(toMap(Function.identity(), $::newArrayList));
		return findParent(source, keyComparator, startMap, lastNode, matchPredicate, (ds, ds2) -> concat(ds2, ds));
	}

	public static <K, D> CompletionStage<FindResult<K, List<D>>> findParentCommits(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                               Set<K> startNodes, Predicate<K> loadPredicate,
	                                                                               AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		Map<K, List<D>> startMap = startNodes.stream().collect(toMap(Function.identity(), k -> new ArrayList<>()));
		return findParent(source, keyComparator, startMap, loadPredicate, matchPredicate, (ds, ds2) -> concat(ds2, ds));
	}

	public static <K, D, A> CompletionStage<FindResult<K, A>> findParent(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                     K startNode, A accumulator, @Nullable K lastNode,
	                                                                     AsyncPredicate<OTCommit<K, D>> matchPredicate,
	                                                                     DiffReducer<A, D> reducer) {
		return findParent(source, keyComparator, singletonMap(startNode, accumulator), lastNode, matchPredicate, reducer);
	}

	public static <K, D, A> CompletionStage<FindResult<K, A>> findParent(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                     Map<K, A> startNodes, @Nullable K lastNode,
	                                                                     AsyncPredicate<OTCommit<K, D>> matchPredicate,
	                                                                     DiffReducer<A, D> reducer) {
		Predicate<K> predicate = lastNode == null
				? k -> true
				: key -> keyComparator.compare(key, lastNode) >= 0;

		return findParent(source, keyComparator, startNodes, predicate, matchPredicate, reducer);
	}

	public static <K, D, A> CompletionStage<FindResult<K, A>> findParent(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                     Map<K, A> startNodes, Predicate<K> loadPredicate,
	                                                                     AsyncPredicate<OTCommit<K, D>> matchPredicate,
	                                                                     DiffReducer<A, D> reducer) {
		PriorityQueue<Entry<K, A>> queue = new PriorityQueue<>(11,
				(o1, o2) -> keyComparator.compare(o2.parent, o1.parent));

		startNodes.forEach((startNode, acc) -> queue.add(new Entry<>(startNode, startNode, acc)));

		return findParent(source, queue, new HashSet<>(), loadPredicate, matchPredicate, reducer);
	}

	private static <K, D, A> CompletionStage<FindResult<K, A>> findParent(OTRemote<K, D> source,
	                                                                      PriorityQueue<Entry<K, A>> queue,
	                                                                      Set<K> visited,
	                                                                      Predicate<K> loadPredicate,
	                                                                      AsyncPredicate<OTCommit<K, D>> matcher,
	                                                                      DiffReducer<A, D> reducer) {
		while (!queue.isEmpty()) {
			Entry<K, A> nodeWithPath = queue.poll();
			K node = nodeWithPath.parent;
			A nodeAccumulator = nodeWithPath.accumulator;
			K child = nodeWithPath.child;
			if (!visited.add(node)) continue;

			return source.loadCommit(node).thenComposeAsync(commit ->
					matcher.test(commit).thenComposeAsync(match -> {
						if (match) {
							K id = commit.getId();
							Set<K> parentIds = commit.getParentIds();
							return Stages.of(FindResult.of(id, child, parentIds, nodeAccumulator));
						}

						for (Map.Entry<K, List<D>> parentEntry : commit.getParents().entrySet()) {
							if (parentEntry.getValue() == null) continue;

							K parent = parentEntry.getKey();
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

	public static <K, D> CompletionStage<K> mergeHeadsAndPush(OTSystem<D> otSystem, OTRemote<K, D> source,
	                                                          Comparator<K> keyComparator) {
		return source.getHeads().thenCompose(heads -> {
			check(!heads.isEmpty(), "Empty heads");

			if (heads.size() == 1) return Stages.of(heads.iterator().next()); // nothing to merge

			return OTMergeAlgorithm.loadAndMerge(otSystem, source, keyComparator, heads).thenCompose(merge ->
					source.createId().thenCompose(mergeCommitId ->
							source.push(singletonList(OTCommit.ofMerge(mergeCommitId, merge)))
									.thenApply($ -> mergeCommitId)));
		});
	}

	public static <K, D> CompletionStage<Set<K>> findSurface(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                         Set<K> startNodes, AsyncPredicate<Set<K>> matchPredicate) {
		PriorityQueue<K> queue = new PriorityQueue<>((o1, o2) -> keyComparator.compare(o2, o1));
		Set<K> surface = new HashSet<>();

		queue.addAll(startNodes);
		surface.addAll(startNodes);

		return findSurface(source, surface, queue, matchPredicate);
	}

	private static <K, D> CompletionStage<Set<K>> findSurface(OTRemote<K, D> source,
	                                                          Set<K> surface, PriorityQueue<K> queue,
	                                                          AsyncPredicate<Set<K>> matchPredicate) {
		return matchPredicate.test(surface).thenCompose(apply -> {
			if (apply) return Stages.of(surface);
			if (queue.isEmpty()) return Stages.of(Collections.emptySet());

			K node = queue.poll();
			surface.remove(node);

			return source.loadCommit(node)
					.thenApply(OTCommit::getParentIds)
					.whenComplete(Stages.onResult(surface::addAll))
					.whenComplete(Stages.onResult(queue::addAll))
					.thenCompose($ -> findSurface(source, surface, queue, matchPredicate));
		});
	}

	public static <K, D> CompletionStage<Optional<K>> findFirstCommonParent(OTRemote<K, D> source,
	                                                                        Comparator<K> keyComparator,
	                                                                        Set<K> parentCandidates) {
		Predicate<Map<K, Set<K>>> matcher = paths -> paths.values().stream()
				.anyMatch(v -> v.size() == parentCandidates.size());

		Map<K, Set<K>> childrenMap = new HashMap<>();
		PriorityQueue<K> queue = new PriorityQueue<>((o1, o2) -> keyComparator.compare(o2, o1));

		queue.addAll(parentCandidates);
		parentCandidates.forEach(node -> childrenMap.put(node, new HashSet<>(singleton(node))));

		return findCommonParents(source, parentCandidates, queue, childrenMap, matcher)
				.thenApply(paths -> paths.entrySet().stream()
						.filter(e -> e.getValue().size() == parentCandidates.size())
						.map(Map.Entry::getKey)
						.findAny());
	}

	public static <K, D> CompletionStage<Set<K>> findCommonParents(OTRemote<K, D> source,
	                                                               Comparator<K> keyComparator,
	                                                               Set<K> parentCandidates) {
		Predicate<Map<K, Set<K>>> matcher = paths -> paths.values().stream()
				.noneMatch(v -> v.size() != parentCandidates.size());

		Map<K, Set<K>> childrenMap = new HashMap<>();
		PriorityQueue<K> queue = new PriorityQueue<>((o1, o2) -> keyComparator.compare(o2, o1));

		queue.addAll(parentCandidates);
		parentCandidates.forEach(node -> childrenMap.put(node, new HashSet<>(singleton(node))));

		return findCommonParents(source, parentCandidates, queue, childrenMap, matcher).thenApply(Map::keySet);
	}

	private static <K, D> CompletionStage<Map<K, Set<K>>> findCommonParents(OTRemote<K, D> source,
	                                                                        Set<K> parentCandidates,
	                                                                        PriorityQueue<K> queue,
	                                                                        Map<K, Set<K>> childrenMap,
	                                                                        Predicate<Map<K, Set<K>>> matcher) {
		logger.debug("search root nodes: queue {}, childrenMap {}", queue, childrenMap);

		if (matcher.test(childrenMap)) return Stages.of(childrenMap);
		if (queue.isEmpty()) return Stages.of(Collections.emptyMap());

		K node = queue.poll();
		Set<K> nodeChildren = childrenMap.remove(node);
		return source.loadCommit(node).thenComposeAsync(commit -> {
			Set<K> parents = commit.getParentIds();
			logger.debug("Commit: {}, parents: {}", node, parents);
			for (K parent : parents) {
				if (!childrenMap.containsKey(parent)) queue.add(parent);
				Set<K> children = childrenMap.computeIfAbsent(parent, k -> new HashSet<>(nodeChildren.size() + 2));

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
		PriorityQueue<ReduceEntry<K, Map<K, A>>> queue = new PriorityQueue<>(11,
				(o1, o2) -> comparator.compare(o2.parent, o1.parent));

		heads.forEach((head, accumulator) -> {
			Map<K, A> map = new HashMap<>();
			map.put(head, accumulator);

			queue.add(new ReduceEntry<>(head, map));
		});

		return reduceEdges(source, queue, commonNode, loadPredicate, reducer, reduceAccumulator)
				.handle((value, throwable) -> (throwable == null && value.keySet().equals(heads.keySet()))
						? Stages.of(value)
						: Stages.<Map<K, A>>ofException(new IllegalArgumentException(
						format("No path from heads `%s` to common node: `%s`", heads.keySet(), commonNode))))
				.thenCompose(Function.identity());
	}

	private static <K, D, A> CompletionStage<Map<K, A>> reduceEdges(OTRemote<K, D> source,
	                                                                PriorityQueue<ReduceEntry<K, Map<K, A>>> queue,
	                                                                K commonNode, Predicate<K> loadPredicate,
	                                                                DiffReducer<A, D> reducer,
	                                                                BiFunction<A, A, A> reduceAccumulator) {
		if (queue.isEmpty()) return Stages.of(new HashMap<K, A>());

		ReduceEntry<K, Map<K, A>> value = queue.poll();
		K key = value.parent;
		Map<K, A> accumulator = value.accumulator;

		while (!queue.isEmpty() && queue.peek().parent.equals(key)) {
			ReduceEntry<K, Map<K, A>> poll = queue.poll();
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
		AsyncPredicate<OTCommit<K, D>> snapshotOrRootPredicate = commit -> commit.isRoot()
				? Stages.of(true)
				: source.isSnapshot(commit.getId());

		return findParentCommits(source, comparator, head, null, snapshotOrRootPredicate).thenCompose(result -> {
			if (!result.isFound()) return Stages.ofException(new OTException("No snapshot or root from id:" + head));

			return source.isSnapshot(result.getCommit()).thenCompose(snapshot -> {
				if (!snapshot) return Stages.of(otSystem.squash(result.getAccumulator()));

				return source.loadSnapshot(result.getCommit()).thenApply(diffs -> {
					List<D> changes = new ArrayList<>(diffs.size() + result.getAccumulator().size());
					changes.addAll(diffs);
					changes.addAll(result.getAccumulator());
					return otSystem.squash(changes);
				});

			});
		});
	}
}
