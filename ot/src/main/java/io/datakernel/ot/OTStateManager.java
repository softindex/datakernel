package io.datakernel.ot;

import io.datakernel.async.AsyncCallable;
import io.datakernel.async.Stages;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.ot.exceptions.OTTransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionStage;

import static io.datakernel.util.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

public final class OTStateManager<K, D> implements EventloopService {
	private final Logger logger = LoggerFactory.getLogger(OTStateManager.class);

	private final Eventloop eventloop;

	private final OTAlgorithms<K, D> algorithms;
	private final OTSystem<D> otSystem;
	private final OTRemote<K, D> remote;
	private final Comparator<K> comparator;

	private K fetchedRevision;
	private List<D> fetchedDiffs = Collections.emptyList();

	private K revision;
	private List<D> workingDiffs = new ArrayList<>();
	private Map<K, OTCommit<K, D>> pendingCommits = new HashMap<>();
	private OTState<D> state;

	OTStateManager(Eventloop eventloop, OTAlgorithms<K, D> algorithms, OTState<D> state) {
		this.eventloop = eventloop;
		this.algorithms = algorithms;
		this.otSystem = algorithms.getOtSystem();
		this.remote = algorithms.getRemote();
		this.comparator = algorithms.getKeyComparator();
		this.state = state;
	}

	public static <K, D> OTStateManager<K, D> create(Eventloop eventloop, OTAlgorithms<K, D> otAlgorithms, OTState<D> state) {
		return new OTStateManager<>(eventloop, otAlgorithms, state);
	}

	private static <D> List<D> concatLists(List<D> a, List<D> b) {
		List<D> diffs = new ArrayList<>(a.size() + b.size());
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
		return remote.getHeads().thenComposeAsync(ks -> {
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
		return algorithms.loadAllChanges(commitId).thenApply(diffs -> {
			apply(diffs);
			fetchedRevision = revision = commitId;
			logger.info("Finish checkout, current revision: {}", revision);
			return revision;
		});
	}

	private final AsyncCallable<K> fetch = AsyncCallable.singleCallOf(this::doFetch);

	public CompletionStage<K> fetch() {
		return fetch.call();
	}

	private CompletionStage<K> doFetch() {
		logger.info("Start fetch with fetched revision and current revision: {}, {}", fetchedRevision, revision);
		K fetchedRevisionCopy = this.fetchedRevision;

		if (pendingCommits.containsKey(fetchedRevisionCopy)) {
			logger.info("Try do fetch before current revision was pushed");
			return Stages.of(null);
		}

		return remote.getHeads().thenCompose(heads -> algorithms.findParent(heads,
				DiffsReducer.toList(),
				commit -> commit.getId().equals(fetchedRevisionCopy),
				null)
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

					List<D> diffs = concatLists(fetchedDiffs, findResult.getAccumulatedDiffs());
					fetchedDiffs = otSystem.squash(diffs);
					fetchedRevision = findResult.getChild();

					logger.info("Finish fetch with fetched revision and current revision: {}, {}",
							fetchedRevision, revision);
					return Stages.of(fetchedRevision);
				}));

	}

	public CompletionStage<K> pull() {
		return fetch().thenCompose($ -> {
			try {
				return Stages.of(rebase());
			} catch (OTTransformException e) {
				invalidateInternalState();
				return Stages.ofException(e);
			}
		});
	}

	public CompletionStage<Boolean> pull(K pullRevision) {
		K currentRevisionId = getRevision();
		return algorithms.findParent(singleton(pullRevision),
				DiffsReducer.toList(),
				commit -> commit.getId().equals(currentRevisionId),
				currentRevisionId)
				.thenCompose(find -> {
					if (!find.isFound()) {
						logger.info("Can`t pull to commit {} from {}", pullRevision, currentRevisionId);
						return Stages.of(false);
					}
					if (currentRevisionId != this.revision) {
						logger.info("Concurrent revisions changes, old {}, new {}", currentRevisionId, this.revision);
						return Stages.of(false);
					}

					fetchedDiffs = otSystem.squash(find.getAccumulatedDiffs());
					fetchedRevision = pullRevision;

					try {
						return Stages.of(rebase());
					} catch (OTTransformException e) {
						invalidateInternalState();
						return Stages.ofException(e);
					}
				})
				.thenApply(o -> true);

	}

	public K rebase() throws OTTransformException {
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

	private final AsyncCallable<K> commit = AsyncCallable.singleCallOf(this::doCommit);

	public CompletionStage<K> commit() {
		return commit.call();
	}

	CompletionStage<K> doCommit() {
		if (workingDiffs.isEmpty()) {
			return Stages.of(null);
		}
		return remote.createCommitId().thenApply(newId -> {
			pendingCommits.put(newId, OTCommit.ofCommit(newId, revision, otSystem.squash(workingDiffs)));
			fetchedRevision = revision = newId;
			fetchedDiffs = Collections.emptyList();
			workingDiffs = new ArrayList<>();
			return newId;
		});
	}

	private final AsyncCallable<Void> push = AsyncCallable.singleCallOf(this::doPush);

	public CompletionStage<Void> push() {
		return push.call();
	}

	CompletionStage<Void> doPush() {
		List<OTCommit<K, D>> list = new ArrayList<>(pendingCommits.values());
		logger.info("Push commits, fetched and current revision: {}, {}", fetchedRevision, revision);
		return remote.push(list).thenAccept($ -> {
			for (OTCommit<K, D> commit : list) {
				pendingCommits.remove(commit.getId());
			}
			logger.info("Finish push commits, fetched and current revision: {}, {}", fetchedRevision, revision);
		});
	}

	public K getRevision() {
		checkState(revision != null);
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

	public OTAlgorithms<K, D> getAlgorithms() {
		return algorithms;
	}

	@Override
	public String toString() {
		return "OTStateManager{" +
				"eventloop=" + eventloop +
				", comparator=" + comparator +
				", fetchedRevision=" + fetchedRevision +
				", fetchedDiffs=" + fetchedDiffs.size() +
				", revision=" + revision +
				", workingDiffs=" + workingDiffs.size() +
				", pendingCommits=" + pendingCommits.size() +
				", state=" + state +
				'}';
	}
}
