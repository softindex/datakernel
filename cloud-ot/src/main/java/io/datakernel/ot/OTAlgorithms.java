package io.datakernel.ot;

import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.StacklessException;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.ot.exceptions.OTException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.async.Promises.toList;
import static io.datakernel.ot.GraphReducer.resume;
import static io.datakernel.ot.GraphReducer.skip;
import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;

public final class OTAlgorithms<K, D> implements EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(OTAlgorithms.class);
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final OTRepository<K, D> repository;
	private final OTSystem<D> otSystem;

	private final PromiseStats findParent = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats findParentLoadCommit = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats findCut = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats findCutLoadCommit = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	OTAlgorithms(Eventloop eventloop, OTSystem<D> otSystem, OTRepository<K, D> source) {
		this.eventloop = eventloop;
		this.otSystem = otSystem;
		this.repository = source;
	}

	public static <K, D> OTAlgorithms<K, D> create(Eventloop eventloop,
			OTSystem<D> otSystem, OTRepository<K, D> source) {
		return new OTAlgorithms<>(eventloop, otSystem, source);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public OTRepository<K, D> getRepository() {
		return repository;
	}

	public OTSystem<D> getOtSystem() {
		return otSystem;
	}

	public OTNode<K, D, OTCommit<K, D>> getOtNode() {
		return OTNodeImpl.create(this);
	}

	public <C> OTNode<K, D, C> getOtNode(
			Function<OTCommit<K, D>, C> commitToObject,
			Function<C, OTCommit<K, D>> objectToCommit) {
		return OTNodeImpl.create(this, commitToObject, objectToCommit);
	}

	public <R> Promise<R> reduce(Set<K> heads, GraphReducer<K, D, R> reducer) {
		return toList(heads.stream().map(repository::loadCommit))
				.thenCompose(headCommits -> {
					PriorityQueue<OTCommit<K, D>> queue = new PriorityQueue<>(reverseOrder(OTCommit::compareTo));
					queue.addAll(headCommits);
					reducer.onStart(unmodifiableCollection(queue));
					return Promise.ofCallback(cb -> walkGraphImpl(reducer, queue, new HashSet<>(heads), cb));
				});
	}

	private <R> void walkGraphImpl(GraphReducer<K, D, R> reducer, PriorityQueue<OTCommit<K, D>> queue,
			Set<K> visited, SettablePromise<R> cb) {
		OTCommit<K, D> commit = queue.peek();
		if (commit == null) {
			cb.setException(GraphExhaustedException.INSTANCE);
			return;
		}
		reducer.onCommit(commit)
				.whenResult(maybeResult -> {
					OTCommit<K, D> polledCommit = queue.poll();
					assert polledCommit == commit;
					if (maybeResult == resume()) {
						toList(commit.getParents().keySet().stream().filter(visited::add).map(repository::loadCommit))
								.whenResult(parentCommits -> {
									queue.addAll(parentCommits);
									walkGraphImpl(reducer, queue, visited, cb);
								})
								.whenException(cb::setException);
					} else if (maybeResult == skip()) {
						walkGraphImpl(reducer, queue, visited, cb);
					} else {
						cb.set(maybeResult);
					}
				})
				.whenException(cb::setException);
	}

	public static final class FindResult<K, A> {
		private final K commit;
		private final Set<K> commitParents;
		private final long commitLevel;
		private final K child;
		private final long childLevel;
		private final A accumulatedDiffs;

		private FindResult(@Nullable K commit, Set<K> commitParents, long commitLevel, K child, long childLevel, A accumulatedDiffs) {
			this.commit = commit;
			this.commitParents = commitParents;
			this.commitLevel = commitLevel;
			this.child = child;
			this.childLevel = childLevel;
			this.accumulatedDiffs = accumulatedDiffs;
		}

		public K getCommit() {
			return commit;
		}

		public K getChild() {
			return child;
		}

		public Long getChildLevel() {
			return checkNotNull(childLevel);
		}

		public Set<K> getCommitParents() {
			return commitParents;
		}

		public long getCommitLevel() {
			return checkNotNull(commitLevel);
		}

		public A getAccumulatedDiffs() {
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

	public <A> Promise<FindResult<K, A>> findParent(Set<K> startNodes,
			DiffsReducer<A, D> diffsReducer,
			AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		return reduce(startNodes,
				new AbstractGraphReducer<K, D, A, FindResult<K, A>>(diffsReducer) {
					@NotNull
					@Override
					protected Promise<Optional<FindResult<K, A>>> tryGetResult(OTCommit<K, D> commit,
							Map<K, Map<K, A>> accumulators, Map<K, OTCommit<K, D>> headCommits) {
						return matchPredicate.test(commit)
								.thenApply(matched -> {
									if (!matched) return Optional.empty();
									Map.Entry<K, A> someHead = accumulators.get(commit.getId()).entrySet().iterator().next();
									return Optional.of(new FindResult<>(
											commit.getId(), commit.getParentIds(), commit.getLevel(),
											someHead.getKey(), headCommits.get(someHead.getKey()).getLevel(),
											someHead.getValue()
									));
								});
					}
				});
	}

	public Promise<K> merge() {
		return repository.getHeads()
				.thenCompose(this::merge)
				.whenComplete(toLogger(logger, thisMethod()));
	}

	static class Tuple<K, D> {
		final Map<K, List<D>> mergeDiffs;
		final long parentsMaxLevel;

		Tuple(Map<K, List<D>> mergeDiffs, long parentsMaxLevel) {
			this.mergeDiffs = mergeDiffs;
			this.parentsMaxLevel = parentsMaxLevel;
		}
	}

	@NotNull
	public Promise<K> merge(@NotNull Set<K> heads) {
		if (heads.size() == 1) return Promise.of(first(heads)); // nothing to merge

		//noinspection OptionalGetWithoutIsPresent
		return Promises.toTuple(Tuple::new,
				loadAndMerge(heads),
				toList(heads.stream()
						.map(repository::loadCommit))
						.thenApply(headCommits -> headCommits.stream()
								.mapToLong(OTCommit::getLevel)
								.max()
								.getAsLong()))
				.thenCompose(tuple -> repository.createCommit(tuple.mergeDiffs, tuple.parentsMaxLevel + 1L))
				.thenCompose(mergeCommit -> repository.push(mergeCommit)
						.thenApply($ -> mergeCommit.getId()));
	}

	public Promise<Set<K>> findCut(Set<K> startNodes,
			Predicate<Collection<OTCommit<K, D>>> matchPredicate) {
		return reduce(startNodes,
				new GraphReducer<K, D, Set<K>>() {
					private Collection<OTCommit<K, D>> queue;

					@Override
					public void onStart(@NotNull Collection<OTCommit<K, D>> queue) {
						this.queue = queue;
					}

					@Override
					public @NotNull Promise<Set<K>> onCommit(@NotNull OTCommit<K, D> commit) {
						if (matchPredicate.test(queue)) {
							return Promise.of(queue.stream().map(OTCommit::getId).collect(toSet()));
						}
						return Promise.of(resume());
					}
				})
				.whenComplete(findCut.recordStats());
	}

	public Promise<K> findAnyCommonParent(Set<K> startCut) {
		return reduce(startCut, new FindAnyCommonParentReducer<>(DiffsReducer.toVoid()))
				.thenApply(Map.Entry::getKey)
				.whenComplete(toLogger(logger, thisMethod(), startCut));
	}

	public Promise<Set<K>> findAllCommonParents(Set<K> startCut) {
		return reduce(startCut, new FindAllCommonParentsReducer<>(DiffsReducer.toVoid()))
				.thenApply(Map::keySet)
				.whenComplete(toLogger(logger, thisMethod(), startCut));
	}

	public Promise<List<D>> diff(K node1, K node2) {
		Set<K> startCut = set(node1, node2);
		return reduce(startCut, new FindAnyCommonParentReducer<>(DiffsReducer.toList()))
				.thenApply(entry -> {
					List<D> diffs1 = entry.getValue().get(node1);
					List<D> diffs2 = entry.getValue().get(node2);
					return concat(diffs2, otSystem.invert(diffs1));
				})
				.whenComplete(toLogger(logger, thisMethod(), startCut));
	}

	public Promise<Set<K>> excludeParents(Set<K> startNodes) {
		checkArgument(!startNodes.isEmpty(), "Start nodes are empty");
		if (startNodes.size() == 1) return Promise.of(startNodes);
		return reduce(startNodes,
				new GraphReducer<K, D, Set<K>>() {
					long minLevel;
					Set<K> nodes = new HashSet<>(startNodes);

					@Override
					public void onStart(@NotNull Collection<OTCommit<K, D>> queue) {
						minLevel = queue.stream().mapToLong(OTCommit::getLevel).min().getAsLong();
					}

					@NotNull
					@Override
					public Promise<Set<K>> onCommit(@NotNull OTCommit<K, D> commit) {
						nodes.removeAll(commit.getParentIds());
						if (commit.getLevel() <= minLevel) {
							return Promise.of(nodes);
						}
						return Promise.of(resume());
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), startNodes));
	}

	private static final class FindAnyCommonParentReducer<K, D, A> extends AbstractGraphReducer<K, D, A, Map.Entry<K, Map<K, A>>> {
		private FindAnyCommonParentReducer(DiffsReducer<A, D> diffsReducer) {
			super(diffsReducer);
		}

		@NotNull
		@Override
		protected Promise<Optional<Map.Entry<K, Map<K, A>>>> tryGetResult(OTCommit<K, D> commit,
				Map<K, Map<K, A>> accumulators, Map<K, OTCommit<K, D>> headCommits) {
			return Promise.of(accumulators.entrySet()
					.stream()
					.filter(entry -> Objects.equals(headCommits.keySet(), entry.getValue().keySet()))
					.findAny()
			);
		}
	}

	private static final class FindAllCommonParentsReducer<K, D, A> extends AbstractGraphReducer<K, D, A, Map<K, Map<K, A>>> {
		private FindAllCommonParentsReducer(DiffsReducer<A, D> diffsReducer) {
			super(diffsReducer);
		}

		@NotNull
		@Override
		protected Promise<Optional<Map<K, Map<K, A>>>> tryGetResult(OTCommit<K, D> commit, Map<K, Map<K, A>> accumulators,
				Map<K, OTCommit<K, D>> headCommits) {
			return Promise.of(
					accumulators.values()
							.stream()
							.map(Map::keySet)
							.allMatch(headCommits.keySet()::equals) ? Optional.of(accumulators) : Optional.empty()
			);
		}
	}

	public <A> Promise<Map<K, A>> reduceEdges(Set<K> heads, K parentNode,
			DiffsReducer<A, D> diffAccumulator) {
		return reduce(heads, new AbstractGraphReducer<K, D, A, Map<K, A>>(diffAccumulator) {
			@NotNull
			@Override
			protected Promise<Optional<Map<K, A>>> tryGetResult(OTCommit<K, D> commit, Map<K, Map<K, A>> accumulators, Map<K, OTCommit<K, D>> headCommits) {
				if (accumulators.containsKey(parentNode)) {
					Map<K, A> toHeads = accumulators.get(parentNode);
					if (Objects.equals(heads, toHeads.keySet())) {
						return Promise.of(Optional.of(toHeads));
					}
				}
				return Promise.of(Optional.empty());
			}
		});
	}

	@SuppressWarnings("unchecked")
	public Promise<List<D>> checkout() {
		List<D>[] cachedSnapshot = new List[1];
		return repository.getHeads()
				.thenCompose(heads ->
						findParent(heads, DiffsReducer.toVoid(),
								commit -> commit.getSnapshotHint() == Boolean.FALSE ?
										Promise.of(false) :
										repository.loadSnapshot(commit.getId())
												.thenApply(maybeSnapshot -> (cachedSnapshot[0] = maybeSnapshot.orElse(null)) != null))
								.thenCompose(findResult -> Promise.of(cachedSnapshot[0])))
				.whenComplete(toLogger(logger, thisMethod()));
	}

	@SuppressWarnings("unchecked")
	public Promise<List<D>> checkout(K commitId) {
		List<D>[] cachedSnapshot = new List[1];
		return repository.getHeads()
				.thenCompose(heads ->
						findParent(union(heads, singleton(commitId)), DiffsReducer.toVoid(),
								commit -> commit.getSnapshotHint() == Boolean.FALSE ?
										Promise.of(false) :
										repository.loadSnapshot(commit.getId())
												.thenApply(maybeSnapshot -> (cachedSnapshot[0] = maybeSnapshot.orElse(null)) != null))
								.thenCompose(findResult -> diff(findResult.commit, commitId)
										.thenApply(diff -> concat(cachedSnapshot[0], diff))))
				.whenComplete(toLogger(logger, thisMethod(), commitId));
	}

	public Promise<Void> saveSnapshot(K revisionId) {
		return checkout(revisionId)
				.thenCompose(diffs -> repository.saveSnapshot(revisionId, diffs));
	}

	private Promise<Map<K, List<D>>> loadAndMerge(Set<K> heads) {
		checkArgument(heads.size() >= 2, "Cannot merge less than 2 heads");
		return loadForMerge(heads)
				.thenCompose(graph -> {
					try {
						Map<K, List<D>> mergeResult = graph.merge(graph.excludeParents(heads));
						if (logger.isTraceEnabled()) {
							logger.info(graph.toGraphViz() + "\n");
						}
						return Promise.of(mergeResult);
					} catch (OTException e) {
						if (logger.isTraceEnabled()) {
							logger.error(graph.toGraphViz() + "\n", e);
						}
						return Promise.ofException(e);
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), heads));
	}

	private class LoadGraphReducer implements GraphReducer<K, D, OTLoadedGraph<K, D>> {
		private final OTLoadedGraph<K, D> graph = new OTLoadedGraph<>(otSystem);
		private final Map<K, Set<K>> head2roots = new HashMap<>();
		private final Map<K, Set<K>> root2heads = new HashMap<>();

		@Override
		public void onStart(@NotNull Collection<OTCommit<K, D>> queue) {
			for (OTCommit<K, D> headCommit : queue) {
				K head = headCommit.getId();
				head2roots.put(head, set(head));
				root2heads.put(head, set(head));
			}
		}

		@Override
		public @NotNull Promise<OTLoadedGraph<K, D>> onCommit(@NotNull OTCommit<K, D> commit) {
			K node = commit.getId();
			Map<K, List<D>> parents = commit.getParents();

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

			graph.addNode(commit);

			if (head2roots.keySet()
					.stream()
					.anyMatch(head -> head2roots.get(head).equals(root2heads.keySet()))) {
				return Promise.of(graph);
			}
			return Promise.of(resume());
		}
	}

	public Promise<OTLoadedGraph<K, D>> loadForMerge(Set<K> heads) {
		return reduce(heads, new LoadGraphReducer())
				.whenComplete(toLogger(logger, thisMethod(), heads));
	}

	public Promise<OTLoadedGraph<K, D>> loadGraph(Set<K> heads, OTLoadedGraph<K, D> graph) {
		return reduce(heads,
				commit -> {
					if (graph.hasVisited(commit.getId())) {
						return Promise.of(skip());
					}
					graph.addNode(commit);
					return Promise.of(resume());
				})
				.thenComposeEx((v, e) -> {
					if (e instanceof GraphExhaustedException) return Promise.of(null);
					return Promise.of(v, e);
				})
				.thenApply($ -> graph)
				.whenComplete(toLogger(logger, thisMethod(), heads, graph));
	}

	public Promise<OTLoadedGraph<K, D>> loadGraph(Set<K> heads) {
		return loadGraph(heads, new OTLoadedGraph<>(otSystem));
	}

	public Promise<OTLoadedGraph<K, D>> loadGraph(Set<K> heads, Function<K, String> idToString, Function<D, String> diffToString) {
		return loadGraph(heads, new OTLoadedGraph<>(otSystem, idToString, diffToString));
	}

	@JmxAttribute
	public PromiseStats getFindParent() {
		return findParent;
	}

	@JmxAttribute
	public PromiseStats getFindParentLoadCommit() {
		return findParentLoadCommit;
	}

	@JmxAttribute
	public PromiseStats getFindCut() {
		return findCut;
	}

	@JmxAttribute
	public PromiseStats getFindCutLoadCommit() {
		return findCutLoadCommit;
	}

}
