package io.datakernel.ot;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Callback;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.StageStats;
import io.datakernel.ot.exceptions.OTException;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;

import static io.datakernel.async.Stages.collectToList;
import static io.datakernel.util.CollectionUtils.concat;
import static io.datakernel.util.CollectionUtils.first;
import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

public final class OTAlgorithms<K, D> implements EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(OTAlgorithms.class);
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final OTRemote<K, D> remote;
	private final Comparator<K> keyComparator;
	private final OTSystem<D> otSystem;
	private final OTMergeAlgorithm<K, D> mergeAlgorithm;

	private final StageStats findParent = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats findParentLoadCommit = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats findCut = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats findCutLoadCommit = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

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

	public <A> Stage<FindResult<K, A>> findParent(Set<K> startNodes,
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

	private <A> Stage<FindResult<K, A>> findParent(PriorityQueue<FindEntry<K, A>> queue,
	                                               Set<K> visited,
	                                               Predicate<K> loadPredicate,
	                                               Predicate<OTCommit<K, D>> matchPredicate,
	                                               DiffsReducer<A, D> diffsAccumulator) {
		SettableStage<FindResult<K, A>> cb = SettableStage.create();
		findParentImpl(queue, visited, loadPredicate, matchPredicate, diffsAccumulator, cb);
		return cb;
	}

	private <A> void findParentImpl(PriorityQueue<FindEntry<K, A>> queue,
	                                Set<K> visited,
	                                Predicate<K> loadPredicate,
	                                Predicate<OTCommit<K, D>> matchPredicate,
	                                DiffsReducer<A, D> diffsAccumulator,
	                                Callback<FindResult<K, A>> cb) {
		while (!queue.isEmpty()) {
			FindEntry<K, A> nodeWithPath = queue.poll();
			K node = nodeWithPath.parent;
			A accumulatedDiffs = nodeWithPath.accumulator;
			K child = nodeWithPath.child;
			if (!visited.add(node)) continue;

			remote.loadCommit(node)
					.whenResult(commit -> {
						if (matchPredicate.test(commit)) {
							K id = commit.getId();
							Set<K> parentIds = commit.getParentIds();
							cb.set(FindResult.of(id, child, parentIds, accumulatedDiffs));
						} else {

							for (Map.Entry<K, List<D>> parentEntry : commit.getParents().entrySet()) {
								if (parentEntry.getValue() == null) continue;

								K parent = parentEntry.getKey();
								if (loadPredicate.test(parent)) {
									A newAccumulatedDiffs = diffsAccumulator.accumulate(accumulatedDiffs, parentEntry.getValue());
									queue.add(new FindEntry<>(parent, child, newAccumulatedDiffs));
								}
							}

							findParentImpl(queue, visited, loadPredicate, matchPredicate, diffsAccumulator, cb);
						}
					})
					.whenException(cb::setException)
					.whenComplete(findParentLoadCommit.recordStats());
			return;
		}

		cb.set(FindResult.notFound());
	}

	public Stage<K> mergeHeadsAndPush() {
		logger.trace("mergeHeadsAndPush");
		Stopwatch sw = Stopwatch.createStarted();
		return remote.getHeads()
				.thenCompose(heads -> {
					check(!heads.isEmpty(), "Empty heads");

					if (heads.size() == 1) return Stage.of(first(heads)); // nothing to merge

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

	public Stage<Set<K>> findCut(Set<K> startNodes,
	                             Predicate<Set<OTCommit<K, D>>> matchPredicate) {
		PriorityQueue<K> queue = new PriorityQueue<>((o1, o2) -> keyComparator.compare(o2, o1));
		queue.addAll(startNodes);
		return findCut(queue, new HashMap<>(), matchPredicate).whenComplete(findCut.recordStats());
	}

	private Stage<Set<K>> findCut(PriorityQueue<K> queue, Map<K, OTCommit<K, D>> queueMap,
	                              Predicate<Set<OTCommit<K, D>>> matchPredicate) {
		SettableStage<Set<K>> result = SettableStage.create();
		findCutImpl(queue, queueMap, matchPredicate, result);
		return result;
	}

	private void findCutImpl(PriorityQueue<K> queue, Map<K, OTCommit<K, D>> queueMap,
	                         Predicate<Set<OTCommit<K, D>>> matchPredicate,
	                         Callback<Set<K>> cb) {
		if (queue.isEmpty()) {
			cb.set(Collections.emptySet());
			return;
		}

		List<Stage<OTCommit<K, D>>> loadStages = queue.stream()
				.filter(revisionId -> !queueMap.containsKey(revisionId))
				.map(revisionId -> remote.loadCommit(revisionId)
						.whenComplete(findCutLoadCommit.recordStats()))
				.collect(toList());

		collectToList(loadStages)
				.whenResult(otCommits -> {
					for (OTCommit<K, D> otCommit : otCommits) {
						queueMap.put(otCommit.getId(), otCommit);
					}
					assert queue.size() == queueMap.size();
					if (matchPredicate.test(new HashSet<>(queueMap.values()))) {
						cb.set(queueMap.keySet());
					} else {
						K node = queue.poll();
						OTCommit<K, D> commit = queueMap.remove(node);
						for (K parentId : commit.getParents().keySet()) {
							if (!queue.contains(parentId)) {
								queue.add(parentId);
							}
						}
						findCutImpl(queue, queueMap, matchPredicate, cb);
					}
				})
				.whenException(cb::setException);
	}

	public Stage<Optional<K>> findFirstCommonParent(Set<K> startCut) {
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

	public Stage<Set<K>> findCommonParents(Set<K> startCut) {
		Predicate<Map<K, Set<K>>> matcher = paths -> paths.values().stream()
				.noneMatch(v -> v.size() != startCut.size());

		Map<K, Set<K>> childrenMap = new HashMap<>();
		PriorityQueue<K> queue = new PriorityQueue<>((o1, o2) -> keyComparator.compare(o2, o1));

		queue.addAll(startCut);
		startCut.forEach(node -> childrenMap.put(node, new HashSet<>(singleton(node))));

		return findCommonParents(startCut, queue, childrenMap, matcher).thenApply(Map::keySet);
	}

	private Stage<Map<K, Set<K>>> findCommonParents(Set<K> cut,
	                                                PriorityQueue<K> queue,
	                                                Map<K, Set<K>> childrenMap,
	                                                Predicate<Map<K, Set<K>>> matcher) {
		SettableStage<Map<K, Set<K>>> result = SettableStage.create();
		findCommonParentsImpl(cut, queue, childrenMap, matcher, result);
		return result;
	}

	private void findCommonParentsImpl(Set<K> cut,
	                                   PriorityQueue<K> queue,
	                                   Map<K, Set<K>> childrenMap,
	                                   Predicate<Map<K, Set<K>>> matcher,
	                                   Callback<Map<K, Set<K>>> cb) {
		logger.debug("search root nodes: queue {}, childrenMap {}", queue, childrenMap);

		if (matcher.test(childrenMap)) {
			cb.set(childrenMap);
			return;
		}
		if (queue.isEmpty()) {
			cb.set(Collections.emptyMap());
			return;
		}

		K node = queue.poll();
		Set<K> nodeChildren = childrenMap.remove(node);
		remote.loadCommit(node)
				.whenResult(commit -> {
					Set<K> parents = commit.getParentIds();
					logger.debug("Commit: {}, parents: {}", node, parents);
					for (K parent : parents) {
						if (!childrenMap.containsKey(parent)) queue.add(parent);
						Set<K> children = childrenMap.computeIfAbsent(parent, $ -> new HashSet<>());

						if (cut.contains(parent)) children.add(parent);
						if (cut.contains(node)) children.add(node);
						children.addAll(nodeChildren);
					}

					findCommonParentsImpl(cut, queue, childrenMap, matcher, cb);
				})
				.whenException(cb::setException);
	}

	private static class ReduceEntry<K, A> {
		public final K node;
		public final Map<K, A> toChildren;

		private ReduceEntry(K parent, Map<K, A> toChildren) {
			this.node = parent;
			this.toChildren = toChildren;
		}
	}

	public <A> Stage<Map<K, A>> reduceEdges(Set<K> heads, K parentNode,
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
				.thenComposeEx((value, throwable) -> {
					if (throwable == null && value.keySet().equals(heads)) {
						return Stage.of(value);
					} else {
						return Stage.ofException(new IllegalArgumentException(
								format("No path from heads `%s` to common node: `%s`", heads, parentNode)));
					}
				});
	}

	private <A> Stage<Map<K, A>> reduceEdges(PriorityQueue<ReduceEntry<K, A>> queue,
	                                         Map<K, ReduceEntry<K, A>> queueMap,
	                                         K commonNode,
	                                         DiffsReducer<A, D> diffAccumulator) {
		SettableStage<Map<K, A>> result = SettableStage.create();
		reduceEdgesImpl(queue, queueMap, commonNode, diffAccumulator, result);
		return result;
	}

	private <A> void reduceEdgesImpl(PriorityQueue<ReduceEntry<K, A>> queue,
	                                 Map<K, ReduceEntry<K, A>> queueMap,
	                                 K commonNode,
	                                 DiffsReducer<A, D> diffAccumulator,
	                                 Callback<Map<K, A>> cb) {
		if (queue.isEmpty()) {
			cb.setException(new IOException());
			return;
		}

		ReduceEntry<K, A> polledEntry = queue.poll();
		queueMap.remove(polledEntry.node);
		if (commonNode.equals(polledEntry.node)) {
			cb.set(polledEntry.toChildren);
			return;
		}
		remote.loadCommit(polledEntry.node)
				.whenResult(commit -> {
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
							A existingAccumulatedDiffs = parentEntry.toChildren.get(child);
							A combinedAccumulatedDiffs = existingAccumulatedDiffs == null ? newAccumulatedDiffs :
									diffAccumulator.combine(existingAccumulatedDiffs, newAccumulatedDiffs);
							parentEntry.toChildren.put(child, combinedAccumulatedDiffs);
						}
					}
					reduceEdgesImpl(queue, queueMap, commonNode, diffAccumulator, cb);
				})
				.whenException(cb::setException);
	}

	public Stage<List<D>> checkout(K head) {
		Predicate<OTCommit<K, D>> snapshotOrRootPredicate = commit -> commit.isRoot() || commit.isSnapshot();

		return findParent(singleton(head), DiffsReducer.toList(), snapshotOrRootPredicate, null)
				.thenCompose(findResult -> {
					if (!findResult.isFound())
						return Stage.ofException(new OTException("No snapshot or root from id:" + head));

					return remote.loadSnapshot(findResult.getCommit()).thenApply(diffs -> {
						List<D> changes = concat(diffs, findResult.getAccumulatedDiffs());
						return otSystem.squash(changes);
					});
				});
	}

	@JmxAttribute
	public StageStats getFindParent() {
		return findParent;
	}

	@JmxAttribute
	public StageStats getFindParentLoadCommit() {
		return findParentLoadCommit;
	}

	@JmxAttribute
	public StageStats getFindCut() {
		return findCut;
	}

	@JmxAttribute
	public StageStats getFindCutLoadCommit() {
		return findCutLoadCommit;
	}

}
