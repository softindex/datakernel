package io.datakernel.ot;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stages;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.ot.OTUtils.FindResult;
import io.datakernel.ot.exceptions.OTTransformException;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import static io.datakernel.async.SettableStage.mirrorOf;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

public final class OTStateManager<K, D> implements EventloopService {
	private final Logger logger = LoggerFactory.getLogger(OTStateManager.class);

	private final Eventloop eventloop;
	private final OTSystem<D> otSystem;
	private final OTRemote<K, D> source;
	private final Comparator<K> comparator;

	private K fetchedRevision;
	private List<D> fetchedDiffs = Collections.emptyList();
	private SettableStage<K> fetchProgress;

	private K revision;
	private List<D> workingDiffs = new ArrayList<>();
	private LinkedHashMap<K, OTCommit<K, D>> pendingCommits = new LinkedHashMap<>();
	private OTState<D> state;

	public OTStateManager(Eventloop eventloop, OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> comparator, OTState<D> state) {
		this.eventloop = eventloop;
		this.otSystem = otSystem;
		this.source = source;
		this.comparator = comparator;
		this.state = state;
	}

	private static <D> List<D> concatLists(List<D> a, List<D> b) {
		final List<D> diffs = new ArrayList<>(a.size() + b.size());
		diffs.addAll(a);
		diffs.addAll(b);
		return diffs;
	}

	public OTState<D> getState() {
		return state;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public CompletionStage<Void> start() {
		return checkout().thenApply(k -> null);
	}

	@Override
	public CompletionStage<Void> stop() {
		invalidateInternalState();
		return Stages.of(null);
	}

	public CompletionStage<K> checkout() {
		logger.info("Start checkout");
		return source.getHeads().thenComposeAsync(ks -> {
			logger.info("Start checkout heads: {}", ks);
			return checkout(ks.iterator().next());
		}).thenCompose($ -> pull());
	}

	public CompletionStage<K> checkout(K commitId) {
		logger.info("Start checkout to commit: {}", commitId);
		fetchedRevision = revision = null;
		workingDiffs.clear();
		pendingCommits.clear();
		state.init();
		fetchedDiffs.clear();
		return OTUtils.loadAllChanges(source, comparator, otSystem, commitId).thenApply(diffs -> {
			apply(diffs);
			fetchedRevision = revision = commitId;
			logger.info("Finish checkout, current revision: {}", revision);
			return revision;
		});
	}

	public CompletionStage<K> fetch() {
		if (fetchProgress != null) {
			logger.info("Reuse fetch in progress");
			return fetchProgress;
		}
		fetchProgress = mirrorOf(doFetch());

		return fetchProgress.whenComplete((aVoid, throwable) -> fetchProgress = null);
	}

	private CompletionStage<K> doFetch() {
		logger.info("Start fetch with fetched revision and current revision: {}, {}", fetchedRevision, revision);
		final K fetchedRevisionCopy = this.fetchedRevision;

		if (pendingCommits.containsKey(fetchedRevisionCopy)) {
			logger.info("Try do fetch before current revision was pushed");
			return Stages.of(null);
		}

		return source.getHeads()
				.thenCompose(heads -> findParentCommits(heads, null, input -> input.getId().equals(fetchedRevisionCopy))
						.thenCompose(findResult -> {
							if (fetchedRevisionCopy != this.fetchedRevision) {
								logger.info("Concurrent fetched revisions changes, old {}, new {}",
										fetchedRevisionCopy, this.fetchedRevision);
								return Stages.of(this.fetchedRevision);
							}

							if (!findResult.isFound()) {
								return Stages.ofException(new IllegalStateException(format(
										"Could not find path from heads to fetched revision and current " +
												"revision: %s, %s, heads: %s", fetchedRevisionCopy, revision, heads)));
							}

							final List<D> diffs = concatLists(fetchedDiffs, findResult.getAccumulator());
							fetchedDiffs = otSystem.squash(diffs);
							fetchedRevision = findResult.getChild();

							logger.info("Finish fetch with fetched revision and current revision: {}, {}",
									fetchedRevision, revision);
							return Stages.of(fetchedRevision);
						}));

	}

	public CompletionStage<K> pull() {
		if (!pendingCommits.isEmpty()) {
			logger.info("Pending commits is not empty, ignore pull");
			return Stages.of(revision);
		}

		return fetch().thenCompose($ -> {
			try {
				return Stages.of(rebase());
			} catch (OTTransformException e) {
				return Stages.ofException(e);
			}
		});
	}

	public CompletionStage<Boolean> pull(K pullRevision) {
		if (!pendingCommits.isEmpty()) {
			logger.info("Pending commits is not empty, ignore pull");
			return Stages.of(false);
		}
		final K revisionCopy = getRevision();
		return findParentCommits(pullRevision, revisionCopy, commit -> commit.getId().equals(revisionCopy))
				.thenCompose(find -> {
					if (!find.isFound()) {
						logger.info("Can`t pull to commit {} from {}", pullRevision, revisionCopy);
						return Stages.of(false);
					}
					if (revisionCopy != this.revision) {
						logger.info("Concurrent revisions changes, old {}, new {}", revisionCopy, this.revision);
						return Stages.of(false);
					}

					fetchedDiffs = otSystem.squash(find.getAccumulator());
					fetchedRevision = pullRevision;

					try {
						return Stages.of(rebase());
					} catch (OTTransformException e) {
						return Stages.ofException(e);
					}
				}).thenApply(o -> true);

	}

	public K rebase() throws OTTransformException {
		if (!pendingCommits.isEmpty()) {
			logger.info("Pending commits is not empty, ignore pull and fetch");
			return revision;
		}

		TransformResult<D> transformed = otSystem.transform(otSystem.squash(workingDiffs), otSystem.squash(fetchedDiffs));
		apply(transformed.left);
		workingDiffs = new ArrayList<>(transformed.right);
		revision = fetchedRevision;
		fetchedDiffs = Collections.emptyList();
		logger.info("Finish rebase, current revision: {}", revision);

		return revision;
	}

	public void reset() {
		List<D> diffs = new ArrayList<>(workingDiffs);
		diffs = otSystem.invert(diffs);
		apply(diffs);
		pendingCommits = new LinkedHashMap<>();
		workingDiffs = new ArrayList<>();
	}

	public CompletionStage<K> commitAndPush() {
		return commit().thenCompose(id -> push().thenApply($ -> id));
	}

	public CompletionStage<K> commit() {
		if (workingDiffs.isEmpty()) {
			return Stages.of(null);
		}
		return source.createId().thenApply(newId -> {
			pendingCommits.put(newId, OTCommit.ofCommit(newId, revision, otSystem.squash(workingDiffs)));
			fetchedRevision = revision = newId;
			fetchedDiffs = Collections.emptyList();
			workingDiffs = new ArrayList<>();
			return newId;
		});
	}

	public CompletionStage<Void> push() {
		if (pendingCommits.isEmpty()) {
			logger.info("Pending commit is not empty, ignore push");
			return Stages.of(null);
		}
		final List<OTCommit<K, D>> list = new ArrayList<>(pendingCommits.values());
		logger.info("Push commits, fetched and current revision: {}, {}", fetchedRevision, revision);
		return source.push(list).thenAccept($ -> {
			list.forEach(commit -> pendingCommits.remove(commit.getId()));
			logger.info("Finish push commits, fetched and current revision: {}, {}", fetchedRevision, revision);
		});
	}

	public K getRevision() {
		return revision;
	}

	public void add(D diff) {
		add(singletonList(diff));
	}

	public void add(List<D> diffs) {
		try {
			for (D diff : diffs) {
				if (!otSystem.isEmpty(diff)) {
					workingDiffs.add(diff);
					state.apply(diff);
				}
			}
		} catch (RuntimeException e) {
			invalidateInternalState();
			throw e;
		}
	}

	private void apply(List<D> diffs) {
		try {
			for (D op : diffs) {
				state.apply(op);
			}
		} catch (RuntimeException e) {
			invalidateInternalState();
			throw e;
		}
	}

	private void invalidateInternalState() {
		fetchedRevision = revision = null;
		fetchedDiffs = workingDiffs = null;
		pendingCommits = null;
		state = null;
	}

	// visible for test
	List<D> getWorkingDiffs() {
		return workingDiffs;
	}

	private boolean isInternalStateValid() {
		return revision != null;
	}

// Helper OT methods

	public CompletionStage<FindResult<K, List<D>>> findParentCommits(K startNode, @Nullable K lastNode,
	                                                                    Predicate<OTCommit<K, D>> matchPredicate) {
		return OTUtils.findParentCommits(source, comparator, startNode, lastNode,
				commit -> Stages.of(matchPredicate.test(commit)));
	}

	public CompletionStage<FindResult<K, List<D>>> findParentCommits(Set<K> startNodes, @Nullable K lastNode,
	                                                                    Predicate<OTCommit<K, D>> matchPredicate) {
		return OTUtils.findParentCommits(source, comparator, startNodes, lastNode,
				commit -> Stages.of(matchPredicate.test(commit)));
	}

	public CompletionStage<K> mergeHeadsAndPush() {
		logger.trace("mergeHeadsAndPush, revision: {}, fetched revision: {}", revision, fetchedRevision);

		final Stopwatch sw = Stopwatch.createStarted();
		return OTUtils.mergeHeadsAndPush(otSystem, source, comparator)
				.whenComplete((k, throwable) -> {
					if (throwable == null) {
						logger.trace("Finish mergeHeadsAndPush in {}, revision: {}, fetched revision: {}",
								sw, revision, fetchedRevision);
					} else {
						logger.trace("Error mergeHeadsAndPush in {}, revision: {}, fetched revision: {}",
								sw, revision, fetchedRevision, throwable);
					}
				});
	}

	@Override
	public String toString() {
		return "OTStateManager{" +
				"eventloop=" + eventloop +
				", comparator=" + comparator +
				", fetchedRevision=" + fetchedRevision +
				", fetchedDiffs=" + fetchedDiffs.size() +
				", fetchProgress=" + fetchProgress +
				", revision=" + revision +
				", workingDiffs=" + workingDiffs.size() +
				", pendingCommits=" + pendingCommits.size() +
				", state=" + state +
				'}';
	}
}
