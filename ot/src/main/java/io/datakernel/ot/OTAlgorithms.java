package io.datakernel.ot;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stages;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.StageStats;
import io.datakernel.ot.exceptions.OTException;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;
import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

public final class OTAlgorithms<K, D> implements EventloopJmxMBean {
	private static final Logger logger = LoggerFactory.getLogger(OTAlgorithms.class);
	public static final double DEFAULT_SMOOTHING_WINDOW = SMOOTHING_WINDOW_5_MINUTES;

	private final Eventloop eventloop;
	private final OTRemote<K, D> remote;
	private final Comparator<K> keyComparator;
	private final OTSystem<D> otSystem;
	private final OTMergeAlgorithm<K, D> mergeAlgorithm;

	private final StageStats findParent = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats findParentRecursive = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats findCut = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats findCutRecursive = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

	OTAlgorithms(Eventloop eventloop, OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator) {
		this.eventloop = eventloop;
		this.otSystem = otSystem;
		this.remote = source;
		this.keyComparator = keyComparator;
		this.mergeAlgorithm = new OTMergeAlgorithm<>(otSystem, source, keyComparator);
	}

	public static <K, D> OTAlgorithms<K, D> create(Eventloop eventloop,
	                                               OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator) {
		return new OTAlgorithms<>(eventloop, otSystem, source, keyComparator);
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public OTRemote<K, D> getRemote() {
		return remote;
	}

	public Comparator<K> getKeyComparator() {
		return keyComparator;
	}

	public OTSystem<D> getOtSystem() {
		return otSystem;
	}

	public static final class FindResult<K, A> {
		@Nullable
		private final K commit;
		@Nullable
		private final Set<K> parents;
		@Nullable
		private final K child;
		@Nullable
		private final A accumulatedDiffs;

		private FindResult(K commit, K child, Set<K> parents, A accumulatedDiffs) {
			this.child = child;
			this.commit = commit;
			this.parents = parents;
			this.accumulatedDiffs = accumulatedDiffs;
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

		public A getAccumulatedDiffs() {
			return checkNotNull(accumulatedDiffs);
		}

		@Override
		public String toString() {
			return "FindResult{" +
					"commit=" + commit +
					", parents=" + parents +
					", child=" + child +
					", accumulator=" + accumulatedDiffs +
					'}';
		}
	}

	private static final class FindEntry<K, A> {
		final K parent;
		final K child;
		final A accumulator;

		private FindEntry(K parent, K child, A accumulator) {
			this.parent = parent;
			this.child = child;
			this.accumulator = accumulator;
		}
	}

	private Predicate<K> loadPredicate(@Nullable K lastNode) {
		return lastNode == null ?
				k -> true :
				key -> keyComparator.compare(key, lastNode) >= 0;
	}

	public <A> CompletionStage<FindResult<K, A>> findParent(Set<K> startNodes,
	                                                        DiffsReducer<A, D> diffAccumulator,
	                                                        Predicate<OTCommit<K, D>> matchPredicate,
	                                                        @Nullable K lastNode) {
		PriorityQueue<FindEntry<K, A>> queue = new PriorityQueue<>(11,
				(o1, o2) -> keyComparator.compare(o2.parent, o1.parent));

		for (K startNode : startNodes) {
			queue.add(new FindEntry<>(startNode, startNode, diffAccumulator.initialValue()));
		}

		Predicate<K> loadPredicate = loadPredicate(lastNode);

		return findParent(queue, new HashSet<>(), loadPredicate, matchPredicate, diffAccumulator).whenComplete(findParent.recordStats());
	}

	private <A> CompletionStage<FindResult<K, A>> findParent(PriorityQueue<FindEntry<K, A>> queue,
	                                                         Set<K> visited,
	                                                         Predicate<K> loadPredicate,
	                                                         Predicate<OTCommit<K, D>> matchPredicate,
	                                                         DiffsReducer<A, D> diffsAccumulator) {
		while (!queue.isEmpty()) {
			FindEntry<K, A> nodeWithPath = queue.poll();
			K node = nodeWithPath.parent;
			A accumulatedDiffs = nodeWithPath.accumulator;
			K child = nodeWithPath.child;
			if (!visited.add(node)) continue;

			return remote.loadCommit(node).thenComposeAsync(commit -> {
				if (matchPredicate.test(commit)) {
					K id = commit.getId();
					Set<K> parentIds = commit.getParentIds();
					return Stages.of(FindResult.of(id, child, parentIds, accumulatedDiffs));
				} else {

					for (Map.Entry<K, List<D>> parentEntry : commit.getParents().entrySet()) {
						if (parentEntry.getValue() == null) continue;

						K parent = parentEntry.getKey();
						if (loadPredicate.test(parent)) {
							A newAccumulatedDiffs = diffsAccumulator.accumulate(accumulatedDiffs, parentEntry.getValue());
							queue.add(new FindEntry<>(parent, child, newAccumulatedDiffs));
						}
					}

					return findParent(queue, visited, loadPredicate, matchPredicate, diffsAccumulator).whenComplete(findParentRecursive.recordStats());
				}
			});
		}

		return Stages.of(FindResult.notFound());
	}

	public CompletionStage<K> mergeHeadsAndPush() {
		logger.trace("mergeHeadsAndPush");
		Stopwatch sw = Stopwatch.createStarted();
		return remote.getHeads()
				.thenCompose(heads -> {
					check(!heads.isEmpty(), "Empty heads");

					if (heads.size() == 1) return Stages.of(heads.iterator().next()); // nothing to merge

					return mergeAlgorithm.loadAndMerge(heads)
							.thenCompose(merge ->
									remote.createCommitId()
											.thenCompose(mergeCommitId ->
													remote.push(singleton(OTCommit.ofMerge(mergeCommitId, merge)))
															.thenApply($ -> mergeCommitId)));
				})
				.whenComplete((k, throwable) -> {
					if (throwable == null) {
						logger.trace("Finish mergeHeadsAndPush in {}", sw);
					} else {
						logger.error("Error mergeHeadsAndPush", throwable);
					}
				});
	}

	public CompletionStage<Set<K>> findCut(Set<K> startNodes,
	                                       Predicate<Set<OTCommit<K, D>>> matchPredicate) {
		PriorityQueue<K> queue = new PriorityQueue<>((o1, o2) -> keyComparator.compare(o2, o1));
		queue.addAll(startNodes);
		return findCut(queue, new HashMap<>(), matchPredicate).whenComplete(findCut.recordStats());
	}

	private CompletionStage<Set<K>> findCut(PriorityQueue<K> queue, Map<K, OTCommit<K, D>> queueMap,
	                                        Predicate<Set<OTCommit<K, D>>> matchPredicate) {
		if (queue.isEmpty()) return Stages.of(Collections.emptySet());

		List<CompletionStage<OTCommit<K, D>>> loadStages = queue.stream()
				.filter(k -> !queueMap.containsKey(k))
				.map(remote::loadCommit)
				.collect(toList());

		return Stages.collect(loadStages).thenComposeAsync(otCommits -> {
			for (OTCommit<K, D> otCommit : otCommits) {
				queueMap.put(otCommit.getId(), otCommit);
			}
			assert queue.size() == queueMap.size();
			if (matchPredicate.test(new HashSet<>(queueMap.values()))) {
				return Stages.of(queueMap.keySet());
			} else {
				K node = queue.poll();
				OTCommit<K, D> commit = queueMap.remove(node);
				for (K parentId : commit.getParents().keySet()) {
					if (!queue.contains(parentId)) {
						queue.add(parentId);
					}
				}
				return findCut(queue, queueMap, matchPredicate).whenComplete(findCutRecursive.recordStats());
			}
		});
	}

	public CompletionStage<Optional<K>> findFirstCommonParent(Set<K> startCut) {
		Predicate<Map<K, Set<K>>> matcher = paths -> paths.values().stream()
				.anyMatch(v -> v.size() == startCut.size());

		Map<K, Set<K>> childrenMap = new HashMap<>();
		PriorityQueue<K> queue = new PriorityQueue<>((o1, o2) -> keyComparator.compare(o2, o1));

		queue.addAll(startCut);
		startCut.forEach(node -> childrenMap.put(node, new HashSet<>(singleton(node))));

		return findCommonParents(startCut, queue, childrenMap, matcher)
				.thenApply(paths -> paths.entrySet().stream()
						.filter(e -> e.getValue().size() == startCut.size())
						.map(Map.Entry::getKey)
						.findAny());
	}

	public CompletionStage<Set<K>> findCommonParents(Set<K> startCut) {
		Predicate<Map<K, Set<K>>> matcher = paths -> paths.values().stream()
				.noneMatch(v -> v.size() != startCut.size());

		Map<K, Set<K>> childrenMap = new HashMap<>();
		PriorityQueue<K> queue = new PriorityQueue<>((o1, o2) -> keyComparator.compare(o2, o1));

		queue.addAll(startCut);
		startCut.forEach(node -> childrenMap.put(node, new HashSet<>(singleton(node))));

		return findCommonParents(startCut, queue, childrenMap, matcher).thenApply(Map::keySet);
	}

	private CompletionStage<Map<K, Set<K>>> findCommonParents(Set<K> cut,
	                                                          PriorityQueue<K> queue,
	                                                          Map<K, Set<K>> childrenMap,
	                                                          Predicate<Map<K, Set<K>>> matcher) {
		logger.debug("search root nodes: queue {}, childrenMap {}", queue, childrenMap);

		if (matcher.test(childrenMap)) return Stages.of(childrenMap);
		if (queue.isEmpty()) return Stages.of(Collections.emptyMap());

		K node = queue.poll();
		Set<K> nodeChildren = childrenMap.remove(node);
		return remote.loadCommit(node).thenComposeAsync(commit -> {
			Set<K> parents = commit.getParentIds();
			logger.debug("Commit: {}, parents: {}", node, parents);
			for (K parent : parents) {
				if (!childrenMap.containsKey(parent)) queue.add(parent);
				Set<K> children = childrenMap.computeIfAbsent(parent, $ -> new HashSet<>());

				if (cut.contains(parent)) children.add(parent);
				if (cut.contains(node)) children.add(node);
				children.addAll(nodeChildren);
			}

			return findCommonParents(cut, queue, childrenMap, matcher);
		});
	}

	private static class ReduceEntry<K, A> {
		public final K node;
		public final Map<K, A> toChildren;

		private ReduceEntry(K parent, Map<K, A> toChildren) {
			this.node = parent;
			this.toChildren = toChildren;
		}
	}

	public <A> CompletionStage<Map<K, A>> reduceEdges(Set<K> heads, K parentNode,
	                                                  DiffsReducer<A, D> diffAccumulator) {
		PriorityQueue<ReduceEntry<K, A>> queue = new PriorityQueue<>(11,
				(o1, o2) -> keyComparator.compare(o2.node, o1.node));
		Map<K, ReduceEntry<K, A>> queueMap = new HashMap<>();

		for (K head : heads) {
			ReduceEntry<K, A> reduceEntry = new ReduceEntry<>(head, new HashMap<>(singletonMap(head, diffAccumulator.initialValue())));
			queue.add(reduceEntry);
			queueMap.put(head, reduceEntry);
		}

		return reduceEdges(queue, queueMap, parentNode, diffAccumulator)
				.handle((value, throwable) ->
						(throwable == null && value.keySet().equals(heads))
								? Stages.of(value)
								: Stages.<Map<K, A>>ofException(new IllegalArgumentException(
								format("No path from heads `%s` to common node: `%s`", heads, parentNode))))
				.thenCompose(identity());
	}

	private <A> CompletionStage<Map<K, A>> reduceEdges(PriorityQueue<ReduceEntry<K, A>> queue,
	                                                   Map<K, ReduceEntry<K, A>> queueMap,
	                                                   K commonNode,
	                                                   DiffsReducer<A, D> diffAccumulator) {
		if (queue.isEmpty()) {
			return Stages.ofException(new IOException());
		}

		ReduceEntry<K, A> polledEntry = queue.poll();
		queueMap.remove(polledEntry.node);
		if (commonNode.equals(polledEntry.node)) {
			return Stages.of(polledEntry.toChildren);
		}
		return remote.loadCommit(polledEntry.node).thenComposeAsync(commit -> {
			for (K parent : commit.getParents().keySet()) {
				if (keyComparator.compare(parent, commonNode) < 0) continue;
				ReduceEntry<K, A> parentEntry = queueMap.get(parent);
				if (parentEntry == null) {
					parentEntry = new ReduceEntry<>(parent, new HashMap<>());
					queueMap.put(parent, parentEntry);
					queue.add(parentEntry);
				}
				for (K child : polledEntry.toChildren.keySet()) {
					A newAccumulatedDiffs = diffAccumulator.accumulate(polledEntry.toChildren.get(child), commit.getParents().get(parent));
					parentEntry.toChildren.put(child, newAccumulatedDiffs);
				}
			}
			return reduceEdges(queue, queueMap, commonNode, diffAccumulator);
		});

	}

	public CompletionStage<List<D>> loadAllChanges(K head) {
		Predicate<OTCommit<K, D>> snapshotOrRootPredicate = commit -> commit.isRoot() || commit.isSnapshot();

		return findParent(singleton(head), DiffsReducer.toList(), snapshotOrRootPredicate, null)
				.thenCompose(findResult -> {
					if (!findResult.isFound())
						return Stages.ofException(new OTException("No snapshot or root from id:" + head));

					return remote.loadSnapshot(findResult.getCommit()).thenApply(diffs -> {
						List<D> changes = new ArrayList<>(diffs.size() + findResult.getAccumulatedDiffs().size());
						changes.addAll(diffs);
						changes.addAll(findResult.getAccumulatedDiffs());
						return otSystem.squash(changes);
					});
				});
	}

	@JmxAttribute
	public StageStats getFindParent() {
		return findParent;
	}

	@JmxAttribute
	public StageStats getFindParentRecursive() {
		return findParentRecursive;
	}

	@JmxAttribute
	public StageStats getFindCut() {
		return findCut;
	}

	@JmxAttribute
	public StageStats getFindCutRecursive() {
		return findCutRecursive;
	}

}
