package io.datakernel.ot;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.time.CurrentTimeProvider;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.async.AsyncSuppliers.subscribe;
import static io.datakernel.util.CollectionUtils.concat;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static java.util.Comparator.comparingLong;

public final class OTNodeImpl<K, D, C> implements OTNode<K, D, C> {
	private static final Logger logger = LoggerFactory.getLogger(OTNodeImpl.class);

	public static final Duration DEFAULT_MERGE_DELAY = Duration.ofMinutes(1);

	private final OTAlgorithms<K, D> algorithms;
	private final OTRepository<K, D> repository;
	private final Function<OTCommit<K, D>, C> commitToObject;
	private final Function<C, OTCommit<K, D>> objectToCommit;
	private final AsyncSupplier<Set<K>> tryMerge = subscribe(this::tryMerge);

	@Nullable
	K lastPushed; // visible for testing
	private long mergeDelay = DEFAULT_MERGE_DELAY.toMillis();
	private long mergeConflictTimestamp;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

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

	public OTNodeImpl<K, D, C> withMergeDelay(Duration mergeDelay) {
		this.mergeDelay = mergeDelay.toMillis();
		return this;
	}

	@Override
	public Promise<C> createCommit(K parent, List<? extends D> diffs, long level) {
		return repository.createCommit(parent, diffs, level)
				.thenApply(commitToObject)
				.whenComplete(toLogger(logger, thisMethod(), parent, diffs, level));
	}

	@Override
	public Promise<K> push(C commit) {
		OTCommit<K, D> otCommit = objectToCommit.apply(commit);
		return repository.push(otCommit)
				.thenApply($ -> {
					lastPushed = otCommit.getId();
					return lastPushed;
				})
				.whenComplete(toLogger(logger, thisMethod(), commit));
	}

	@Override
	public Promise<FetchData<K, D>> checkout() {
		@SuppressWarnings("unchecked") List<D>[] cachedSnapshot = new List[]{null};
		return repository.getHeads()
				.thenCompose(heads -> algorithms.findParent(
						heads,
						DiffsReducer.toList(),
						commit -> commit.getSnapshotHint() == Boolean.FALSE ?
								Promise.of(false) :
								repository.loadSnapshot(commit.getId())
										.thenApply(maybeSnapshot -> (cachedSnapshot[0] = maybeSnapshot.orElse(null)) != null)
				))
				.thenCompose(findResult -> Promise.of(
						new FetchData<>(
								findResult.getChild(),
								findResult.getChildLevel(),
								concat(cachedSnapshot[0], findResult.getAccumulatedDiffs()))))
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
		return tryMerge.get()
				.thenCompose(heads -> algorithms.findParent(
						heads,
						DiffsReducer.toList(),
						commit -> Promise.of(commit.getId().equals(currentCommitId))
				))
				.thenApply(findResult -> new FetchData<>(
						findResult.getChild(),
						findResult.getChildLevel(),
						algorithms.getOtSystem().squash(findResult.getAccumulatedDiffs())
				))
				.whenComplete(toLogger(logger, thisMethod(), currentCommitId));
	}

	public Promise<Set<K>> tryMerge() {
		return repository.getHeads()
				.thenCompose(heads -> {
					if (heads.size() == 1) {
						mergeConflictTimestamp = 0;
						return Promise.of(heads);
					}

					if (mergeConflictTimestamp != 0 && now.currentTimeMillis() > mergeConflictTimestamp + mergeDelay) {
						return doMerge(heads);
					}
					return Promises.toList(heads.stream().map(repository::loadCommit))
							.thenCompose(commits -> {
								K earliestCommit = commits.stream()
										.min(comparingLong(OTCommit::getTimestamp))
										.map(OTCommit::getId)
										.orElse(null);

								if (earliestCommit == null || lastPushed == null || lastPushed.equals(earliestCommit)) {
									return doMerge(heads);
								}

								if (mergeConflictTimestamp == 0) {
									mergeConflictTimestamp = now.currentTimeMillis();
								}
								return Promise.of(heads);
							});
				});
	}

	private Promise<Set<K>> doMerge(Set<K> heads) {
		return algorithms.merge(heads)
				.whenResult(mergeCommit -> {
					lastPushed = mergeCommit;
					mergeConflictTimestamp = 0;
				})
				.thenApply(Collections::singleton);
	}
}
