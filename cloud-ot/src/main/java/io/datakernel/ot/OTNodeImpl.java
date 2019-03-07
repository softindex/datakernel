package io.datakernel.ot;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static java.util.Collections.singleton;

public final class OTNodeImpl<K, D, C> implements OTNode<K, D, C> {
	private static final Logger logger = LoggerFactory.getLogger(OTNodeImpl.class);
	public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(1000L);

	private final OTAlgorithms<K, D> algorithms;
	private final OTRepository<K, D> repository;
	private final Function<OTCommit<K, D>, C> commitToObject;
	private final Function<C, OTCommit<K, D>> objectToCommit;

	private Duration pollInterval = DEFAULT_POLL_INTERVAL;

	private OTNodeImpl(OTAlgorithms<K, D> algorithms, Function<OTCommit<K, D>, C> commitToObject, Function<C, OTCommit<K, D>> objectToCommit) {
		this.algorithms = algorithms;
		this.repository = algorithms.getRepository();
		this.commitToObject = commitToObject;
		this.objectToCommit = objectToCommit;
	}

	public static <K, D, C> OTNodeImpl<K, D, C> create(OTAlgorithms<K, D> algorithms, Function<OTCommit<K, D>, C> commitToObject, Function<C, OTCommit<K,
			D>> objectToCommit) {
		return new OTNodeImpl<>(algorithms, commitToObject, objectToCommit);
	}

	public static <K, D> OTNodeImpl<K, D, OTCommit<K, D>> create(OTAlgorithms<K, D> algorithms) {
		return new OTNodeImpl<>(algorithms, commit -> commit, object -> object);
	}

	public OTNodeImpl<K, D, C> withPollInterval(Duration pollInterval) {
		this.pollInterval = pollInterval;
		return this;
	}

	@Override
	public Promise<C> createCommit(K parent, List<? extends D> diffs, long level) {
		return repository.createCommit(parent, diffs, level)
				.thenApply(commitToObject)
				.whenComplete(toLogger(logger, thisMethod(), parent, diffs, level));
	}

	@Override
	public Promise<FetchData<K, D>> push(C commit) {
		OTCommit<K, D> otCommit = objectToCommit.apply(commit);
		return repository.push(otCommit)
				.thenCompose($ -> repository.getHeads())
				.thenCompose(initalHeads -> algorithms.excludeParents(union(initalHeads, singleton(otCommit.getId())))
						.thenCompose(algorithms::merge)
						.thenCompose(mergeHead -> {
							Set<K> mergeHeadSet = singleton(mergeHead);
							return repository.updateHeads(mergeHeadSet, difference(initalHeads, mergeHeadSet))
									.thenCompose($ -> doFetch(mergeHeadSet, otCommit.getId()));
						}))
				.whenComplete(toLogger(logger, thisMethod(), commit));
	}

	@Override
	public Promise<FetchData<K, D>> checkout() {
		@SuppressWarnings("unchecked") List<D>[] cachedSnapshotRef = new List[]{null};
		return repository.getHeads()
				.thenCompose(heads -> algorithms.findParent(
						heads,
						DiffsReducer.toList(),
						commit -> commit.getSnapshotHint() == Boolean.FALSE ?
								Promise.of(false) :
								repository.loadSnapshot(commit.getId())
										.thenApply(maybeSnapshot -> (cachedSnapshotRef[0] = maybeSnapshot.orElse(null)) != null)
				))
				.thenCompose(findResult -> Promise.of(
						new FetchData<>(
								findResult.getChild(),
								findResult.getChildLevel(),
								concat(cachedSnapshotRef[0], findResult.getAccumulatedDiffs()))))
				.thenCompose(checkoutData -> fetch(checkoutData.getCommitId())
						.thenApply(fetchData -> new FetchData<>(
								fetchData.getCommitId(),
								fetchData.getLevel(),
								algorithms.getOtSystem().squash(concat(checkoutData.getDiffs(), fetchData.getDiffs()))
						))
				)
				.whenComplete(toLogger(logger, thisMethod()));
	}

	@Override
	public Promise<FetchData<K, D>> fetch(K currentCommitId) {
		return repository.getHeads()
				.thenCompose(heads -> doFetch(heads, currentCommitId));
	}

	@Override
	public Promise<FetchData<K, D>> poll(K currentCommitId) {
		Duration[] pollIntervalRef = new Duration[]{Duration.ZERO};
		return repository.getHeads()
				.thenCompose(initialHeads ->
						Promises.until(initialHeads,
								heads -> repository.pollHeads(heads)
										.whenResult(newHeads ->
												pollIntervalRef[0] = Objects.equals(heads, newHeads) ?
														pollInterval :
														Duration.ZERO),
								heads -> heads.contains(currentCommitId) ?
										Promises.delay(Promise.of(false), pollIntervalRef[0]) :
										Promise.of(true)))
				.thenCompose(heads -> doFetch(heads, currentCommitId));
	}

	public Promise<FetchData<K, D>> doFetch(Set<K> heads, K currentCommitId) {
		return algorithms.findParent(
				heads,
				DiffsReducer.toList(),
				commit -> Promise.of(commit.getId().equals(currentCommitId)))
				.thenApply(findResult -> new FetchData<>(
						findResult.getChild(),
						findResult.getChildLevel(),
						algorithms.getOtSystem().squash(findResult.getAccumulatedDiffs())
				))
				.whenComplete(toLogger(logger, thisMethod(), currentCommitId));
	}
}
