package io.datakernel.ot;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.StageStats;
import io.datakernel.ot.exceptions.OTException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;

import static io.datakernel.async.Stages.toList;
import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.*;
import static java.util.Collections.*;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class OTAlgorithms<K, D> implements EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(OTAlgorithms.class);
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final OTRemote<K, D> remote;
	private final OTSystem<D> otSystem;

	private final StageStats findParent = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats findParentLoadCommit = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats findCut = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats findCutLoadCommit = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

	OTAlgorithms(Eventloop eventloop, OTSystem<D> otSystem, OTRemote<K, D> source) {
		this.eventloop = eventloop;
		this.otSystem = otSystem;
		this.remote = source;
	}

	public static <K, D> OTAlgorithms<K, D> create(Eventloop eventloop,
			OTSystem<D> otSystem, OTRemote<K, D> source) {
		return new OTAlgorithms<>(eventloop, otSystem, source);
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public OTRemote<K, D> getRemote() {
		return remote;
	}

	public OTSystem<D> getOtSystem() {
		return otSystem;
	}

	public interface GraphWalker<K, D, R> {
		void onStart(List<OTCommit<K, D>> commits);

		Stage<Optional<R>> onCommit(OTCommit<K, D> commit);
	}

	public static abstract class SimpleGraphWalker<K, D, R> implements GraphWalker<K, D, R> {
		@Nullable
		protected abstract R handleCommit(OTCommit<K, D> commit) throws Exception;

		@Override
		public final Stage<Optional<R>> onCommit(OTCommit<K, D> commit) {
			try {
				R result = handleCommit(commit);
				return result != null ? Stage.of(Optional.of(result)) : Stage.of(Optional.empty());
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				return Stage.ofException(e);
			}
		}
	}

	public <R> Stage<R> walkGraph(Set<K> heads, GraphWalker<K, D, R> walker) {
		return toList(heads.stream().map(remote::loadCommit))
				.thenCompose(headCommits -> {
					walker.onStart(headCommits);
					PriorityQueue<OTCommit<K, D>> queue = new PriorityQueue<>(reverseOrder(OTCommit::compareTo));
					queue.addAll(headCommits);
					return walkGraphImpl(walker, queue, new HashSet<>(heads));
				});
	}

	private <R> Stage<R> walkGraphImpl(GraphWalker<K, D, R> walker, PriorityQueue<OTCommit<K, D>> queue,
			Set<K> visited) {
		if (queue.isEmpty()) {
			return Stage.ofException(new OTException("Incomplete graph"));
		}

		OTCommit<K, D> commit = queue.poll();
		assert commit != null;

		return walker.onCommit(commit)
				.thenCompose(maybeResult -> maybeResult.isPresent() ?
						Stage.of(maybeResult.get()) :
						toList(commit.getParents().keySet().stream().filter(visited::add).map(remote::loadCommit))
								.post()
								.thenCompose(parentCommits -> {
									queue.addAll(parentCommits);
									return walkGraphImpl(walker, queue, visited);
								}));

	}

	public static final class FindResult<K, A> {
		private final K commit;
		private final Set<K> commitParents;
		private final long commitLevel;
		private final K child;
		private final long childLevel;
		private final A accumulatedDiffs;

		private FindResult(K commit, K child, Set<K> commitParents, long commitLevel, long childLevel, A accumulatedDiffs) {
			this.child = child;
			this.commit = commit;
			this.commitParents = commitParents;
			this.commitLevel = commitLevel;
			this.childLevel = childLevel;
			this.accumulatedDiffs = accumulatedDiffs;
		}

		public static <K, A> FindResult<K, A> of(K commit, K child, Set<K> parents, long commitLevel, long childLevel, A accumulator) {
			return new FindResult<>(commit, child, parents, commitLevel, childLevel, accumulator);
		}

		public static <K, A> FindResult<K, A> notFound() {
			return new FindResult<>(null, null, null, 0, 0, null);
		}

		public boolean isFound() {
			return commit != null;
		}

		public K getCommit() {
			checkState(isFound());
			return commit;
		}

		public K getChild() {
			checkState(isFound());
			return child;
		}

		public Long getChildLevel() {
			checkState(isFound());
			return checkNotNull(childLevel);
		}

		public Set<K> getCommitParents() {
			checkState(isFound());
			return commitParents;
		}

		public long getCommitLevel() {
			checkState(isFound());
			return checkNotNull(commitLevel);
		}

		public A getAccumulatedDiffs() {
			checkState(isFound());
			return accumulatedDiffs;
		}

		@Override
		public String toString() {
			return "FindResult{" +
					"commit=" + commit +
					", parents=" + commitParents +
					", child=" + child +
					", accumulator=" + accumulatedDiffs +
					'}';
		}
	}

	public <A> Stage<FindResult<K, A>> findParent(Set<K> startNodes,
			DiffsReducer<A, D> diffAccumulator,
			AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		return walkGraph(startNodes,
				new FindParentWalker<>(startNodes, matchPredicate, diffAccumulator));
	}

	private static class FindParentWalker<K, D, A> implements GraphWalker<K, D, FindResult<K, A>> {
		private static final class FindEntry<K, A> {
			final K parent;
			final K child;
			final A accumulator;
			Long childLevel;

			private FindEntry(K parent, K child, A accumulator, Long childLevel) {
				this.parent = parent;
				this.child = child;
				this.accumulator = accumulator;
				this.childLevel = childLevel;
			}
		}

		private final AsyncPredicate<OTCommit<K, D>> matchPredicate;
		private final DiffsReducer<A, D> diffsReducer;
		private final HashMap<K, FindEntry<K, A>> entries = new HashMap<>();

		private FindParentWalker(Set<K> startNodes, AsyncPredicate<OTCommit<K, D>> matchPredicate, DiffsReducer<A, D> diffsReducer) {
			this.matchPredicate = matchPredicate;
			this.diffsReducer = diffsReducer;
			for (K startNode : startNodes) {
				entries.put(startNode, new FindEntry<>(startNode, startNode, diffsReducer.initialValue(), null));
			}
		}

		@Override
		public void onStart(List<OTCommit<K, D>> otCommits) {
		}

		@Override
		public Stage<Optional<FindResult<K, A>>> onCommit(OTCommit<K, D> commit) {
			K node = commit.getId();
			FindEntry<K, A> nodeWithPath = entries.get(node);
			if (nodeWithPath.childLevel == null) {
				assert nodeWithPath.child.equals(commit.getId());
				nodeWithPath.childLevel = commit.getLevel();
			}
			A accumulatedDiffs = nodeWithPath.accumulator;

			return matchPredicate.test(commit)
					.thenApply(testResult -> {
								if (testResult) {
									return Optional.of(FindResult.of(commit.getId(), nodeWithPath.child, commit.getParentIds(), commit.getLevel(), nodeWithPath.childLevel, accumulatedDiffs));
								}

								for (Map.Entry<K, List<D>> parentEntry : commit.getParents().entrySet()) {
									if (parentEntry.getValue() == null) continue;

									K parent = parentEntry.getKey();
									A newAccumulatedDiffs = diffsReducer.accumulate(accumulatedDiffs, parentEntry.getValue());
									entries.put(parent, new FindEntry<>(parent, nodeWithPath.child, newAccumulatedDiffs, nodeWithPath.childLevel));
								}

								return Optional.empty();
							}
					);
		}
	}

	static class Tuple<K, D> {
		final Map<K, List<D>> mergeDiffs;
		final long parentsMaxLevel;

		Tuple(Map<K, List<D>> mergeDiffs, long parentsMaxLevel) {
			this.mergeDiffs = mergeDiffs;
			this.parentsMaxLevel = parentsMaxLevel;
		}
	}

	@SuppressWarnings("ConstantConditions")
	public Stage<K> mergeHeadsAndPush() {
		return remote.getHeads()
				.thenCompose(heads -> {
					if (heads.size() == 1) return Stage.of(first(heads)); // nothing to merge

					return Stages.toTuple(Tuple::new,
							loadAndMerge(heads),
							toList(heads.stream()
									.map(remote::loadCommit))
									.thenApply(headCommits -> headCommits.stream()
											.mapToLong(OTCommit::getLevel)
											.max()
											.getAsLong()))
							.thenCompose(tuple -> remote.createCommit(tuple.mergeDiffs, tuple.parentsMaxLevel + 1L))
							.thenCompose(mergeCommit -> remote.push(mergeCommit)
									.thenApply($ -> mergeCommit.getId()));
				})
				.whenComplete(toLogger(logger, thisMethod()));
	}

	public Stage<Set<K>> findCut(Set<K> startNodes,
			Predicate<Set<OTCommit<K, D>>> matchPredicate) {
		return toList(startNodes.stream().map(remote::loadCommit))
				.thenCompose(commits -> {
					PriorityQueue<OTCommit<K, D>> queue = new PriorityQueue<>(reverseOrder(OTCommit::compareTo));
					queue.addAll(commits);
					return findCutImpl(queue, new HashSet<>(startNodes), matchPredicate);
				})
				.whenComplete(findCut.recordStats());
	}

	private Stage<Set<K>> findCutImpl(PriorityQueue<OTCommit<K, D>> queue, Set<K> visited,
			Predicate<Set<OTCommit<K, D>>> matchPredicate) {
		if (queue.isEmpty()) {
			return Stage.ofException(new NoSuchElementException());
		}

		HashSet<OTCommit<K, D>> commits = new HashSet<>(queue);
		if (matchPredicate.test(commits)) {
			return Stage.of(commits.stream().map(OTCommit::getId).collect(toSet()));
		}

		OTCommit<K, D> commit = queue.poll();
		assert commit != null;
		return toList(commit.getParents().keySet().stream().filter(visited::add).map(remote::loadCommit))
				.post()
				.thenCompose(parentCommits -> {
					queue.addAll(parentCommits);
					return findCutImpl(queue, visited, matchPredicate);
				});
	}

	public Stage<K> findAnyCommonParent(Set<K> startCut) {
		return walkGraph(startCut, new FindAnyCommonParentWalker<>(startCut))
				.whenComplete(toLogger(logger, thisMethod(), startCut));
	}

	public Stage<Set<K>> findAllCommonParents(Set<K> startCut) {
		return walkGraph(startCut, new FindAllCommonParentsWalker<>(startCut))
				.whenComplete(toLogger(logger, thisMethod(), startCut));
	}

	public Stage<Set<K>> excludeParents(Set<K> startNodes) {
		checkArgument(!startNodes.isEmpty());
		if (startNodes.size() == 1) return Stage.of(startNodes);
		return walkGraph(startNodes, new GraphWalker<K, D, Set<K>>() {
			long minLevel;
			Set<K> nodes = new HashSet<>(startNodes);

			@SuppressWarnings("ConstantConditions")
			@Override
			public void onStart(List<OTCommit<K, D>> otCommits) {
				minLevel = otCommits.stream().mapToLong(OTCommit::getLevel).min().getAsLong();
			}

			@Override
			public Stage<Optional<Set<K>>> onCommit(OTCommit<K, D> commit) {
				nodes.removeAll(commit.getParentIds());
				if (commit.getLevel() <= minLevel)
					return Stage.of(Optional.of(nodes));
				return Stage.of(Optional.empty());
			}
		}).whenComplete(toLogger(logger, thisMethod(), startNodes));
	}

	private static abstract class AbstractFindCommonParentWalker<K, D, R> extends SimpleGraphWalker<K, D, R> {
		protected final Set<K> heads;
		protected final Map<K, Set<K>> nodeToHeads;

		protected AbstractFindCommonParentWalker(Set<K> heads) {
			this.heads = heads;
			this.nodeToHeads = heads.stream().collect(toMap(identity(), head -> new HashSet<>(singleton(head))));
		}

		@Nullable
		protected abstract R tryGetResult();

		@Override
		public void onStart(List<OTCommit<K, D>> otCommits) {
		}

		@Override
		protected R handleCommit(OTCommit<K, D> commit) {
			if (heads.size() == 1) {
				return checkNotNull(tryGetResult());
			}

			K commitId = commit.getId();
			Set<K> commitHeads = nodeToHeads.remove(commitId);

			for (K parent : commit.getParentIds()) {
				nodeToHeads.computeIfAbsent(parent, $ -> new HashSet<>())
						.addAll(commitHeads);
			}

			return tryGetResult();
		}
	}

	private static final class FindAnyCommonParentWalker<K, D> extends AbstractFindCommonParentWalker<K, D, K> {
		protected FindAnyCommonParentWalker(Set<K> heads) {
			super(heads);
		}

		@Override
		protected K tryGetResult() {
			return nodeToHeads.entrySet().stream()
					.filter(entry -> heads.equals(entry.getValue()))
					.findAny()
					.map(Map.Entry::getKey)
					.orElse(null);
		}
	}

	private static final class FindAllCommonParentsWalker<K, D> extends AbstractFindCommonParentWalker<K, D, Set<K>> {
		protected FindAllCommonParentsWalker(Set<K> heads) {
			super(heads);
		}

		@Override
		protected Set<K> tryGetResult() {
			return nodeToHeads.values().stream().allMatch(heads::equals) ?
					nodeToHeads.keySet() : null;
		}
	}

	public <A> Stage<Map<K, A>> reduceEdges(Set<K> heads, K parentNode,
			DiffsReducer<A, D> diffAccumulator) {
		return walkGraph(heads, new ReduceEdgesWalker<>(heads, parentNode, diffAccumulator));
	}

	private static final class ReduceEdgesWalker<K, D, A> extends SimpleGraphWalker<K, D, Map<K, A>> {
		private static class ReduceEntry<K, A> {
			public final K node;
			public final Map<K, A> toChildren;

			private ReduceEntry(K parent, Map<K, A> toChildren) {
				this.node = parent;
				this.toChildren = toChildren;
			}
		}

		private final K parentNode;
		private final DiffsReducer<A, D> diffAccumulator;
		private final Map<K, ReduceEntry<K, A>> queueMap = new HashMap<>();

		public ReduceEdgesWalker(Set<K> heads, K parentNode, DiffsReducer<A, D> diffAccumulator) {
			this.parentNode = parentNode;
			this.diffAccumulator = diffAccumulator;
			for (K head : heads) {
				queueMap.put(head,
						new ReduceEntry<>(head, new HashMap<>(singletonMap(head, diffAccumulator.initialValue()))));
			}
		}

		@Override
		public void onStart(List<OTCommit<K, D>> otCommits) {
		}

		@Override
		protected Map<K, A> handleCommit(OTCommit<K, D> commit) {
			ReduceEntry<K, A> polledEntry = queueMap.remove(commit.getId());
			assert polledEntry.node.equals(commit.getId());
			if (parentNode.equals(commit.getId())) {
				return polledEntry.toChildren;
			}

			for (K parent : commit.getParents().keySet()) {
//				if (keyComparator.compare(parent, parentNode) < 0) continue;
				ReduceEntry<K, A> parentEntry = queueMap.get(parent);
				if (parentEntry == null) {
					parentEntry = new ReduceEntry<>(parent, new HashMap<>());
					queueMap.put(parent, parentEntry);
				}
				for (K child : polledEntry.toChildren.keySet()) {
					A newAccumulatedDiffs = diffAccumulator.accumulate(polledEntry.toChildren.get(child), commit.getParents().get(parent));
					A existingAccumulatedDiffs = parentEntry.toChildren.get(child);
					A combinedAccumulatedDiffs = existingAccumulatedDiffs == null ? newAccumulatedDiffs :
							diffAccumulator.combine(existingAccumulatedDiffs, newAccumulatedDiffs);
					parentEntry.toChildren.put(child, combinedAccumulatedDiffs);
				}
			}

			return null;
		}
	}

	@SuppressWarnings({"SimplifiableConditionalExpression", "ConstantConditions", "OptionalIsPresent", "unchecked"})
	public Stage<List<D>> checkout(K commitId) {
		List<D>[] cachedSnapshot = new List[1];
		return findParent(singleton(commitId), DiffsReducer.toList(),
				commit -> commit.getSnapshotHint() == Boolean.FALSE ?
						Stage.of(false) :
						remote.loadSnapshot(commit.getId())
								.thenApply(maybeSnapshot -> (cachedSnapshot[0] = maybeSnapshot.orElse(null)) != null))
				.thenCompose(findResult -> {
					if (!findResult.isFound())
						return Stage.ofException(new OTException("No snapshot or root from id:" + commitId));

					List<D> changes = concat(cachedSnapshot[0], findResult.getAccumulatedDiffs());
					return Stage.of(otSystem.squash(changes));
				})
				.whenComplete(toLogger(logger, thisMethod(), commitId));
	}

	public Stage<Void> saveSnapshot(K revisionId) {
		return checkout(revisionId)
				.thenCompose(diffs -> remote.saveSnapshot(revisionId, diffs));
	}

	private Stage<Map<K, List<D>>> loadAndMerge(Set<K> heads) {
		checkArgument(heads.size() >= 2);
		return loadGraph(heads)
				.thenCompose(graph -> {
					try {
						Map<K, List<D>> mergeResult = graph.merge(graph.excludeParents(heads));
						if (logger.isTraceEnabled()) {
							logger.info(graph.toGraphViz() + "\n");
						}
						return Stage.of(mergeResult);
					} catch (OTException e) {
						if (logger.isTraceEnabled()) {
							logger.error(graph.toGraphViz() + "\n", e);
						}
						return Stage.ofException(e);
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), heads));
	}

	private class LoadGraphWalker extends SimpleGraphWalker<K, D, OTLoadedGraph<K, D>> {
		private final OTLoadedGraph<K, D> graph = new OTLoadedGraph<>(otSystem);
		private final Map<K, Set<K>> head2roots = new HashMap<>();
		private final Map<K, Set<K>> root2heads = new HashMap<>();

		private LoadGraphWalker(Set<K> heads) {
			for (K head : heads) {
				head2roots.put(head, set(head));
				root2heads.put(head, set(head));
			}
		}

		@Override
		public void onStart(List<OTCommit<K, D>> otCommits) {
		}

		@Override
		protected OTLoadedGraph<K, D> handleCommit(OTCommit<K, D> commit) {
			K node = commit.getId();
			Map<K, List<D>> parents = commit.getParents();

			graph.setNodeTimestamp(commit.getId(), commit.getTimestamp());

			Set<K> affectedHeads = root2heads.remove(node);
			for (K affectedHead : affectedHeads) {
				head2roots.get(affectedHead).remove(node);
			}
			for (K parent : commit.isRoot() ? singleton(node) : parents.keySet()) {
				Set<K> parentRoots = graph.findRoots(parent);
				for (K affectedHead : affectedHeads) {
					head2roots.computeIfAbsent(affectedHead, $ -> new HashSet<>()).addAll(parentRoots);
				}
				for (K parentRoot : parentRoots) {
					root2heads.computeIfAbsent(parentRoot, $ -> new HashSet<>()).addAll(affectedHeads);
				}
			}

			for (K parent : parents.keySet()) {
				graph.addEdge(parent, node, parents.get(parent));
			}

			return head2roots.keySet().stream()
					.anyMatch(head -> head2roots.get(head).equals(root2heads.keySet())) ?
					graph : null;
		}
	}

	public Stage<OTLoadedGraph<K, D>> loadGraph(Set<K> heads) {
		return walkGraph(heads, new LoadGraphWalker(heads))
//				.whenException(throwable -> {
//					if (logger.isTraceEnabled()) {
//						logger.error(graph.toGraphViz() + "\n", throwable);
//					}
//				})
				.whenComplete(toLogger(logger, thisMethod(), heads));
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
