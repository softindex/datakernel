package io.datakernel.ot;

import io.datakernel.async.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static io.datakernel.util.CollectionUtils.concat;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;

public final class OTNodeImpl<K, D> implements OTNode<K, D> {
	private static final Logger logger = LoggerFactory.getLogger(OTAlgorithms.class);

	private final OTAlgorithms<K, D> algorithms;
	private final OTRepository<K, D> repository;
	private final Function<OTCommit<K, D>, Object> commitToObject;
	private final Function<Object, OTCommit<K, D>> objectToCommit;

	private OTNodeImpl(OTAlgorithms<K, D> algorithms, Function<OTCommit<K, D>, Object> commitToObject, Function<Object, OTCommit<K, D>> objectToCommit) {
		this.algorithms = algorithms;
		this.repository = algorithms.getRepository();
		this.commitToObject = commitToObject;
		this.objectToCommit = objectToCommit;
	}

	public static <K, D> OTNodeImpl<K, D> create(OTAlgorithms<K, D> algorithms, Function<OTCommit<K, D>, Object> commitToObject, Function<Object, OTCommit<K,
			D>> objectToCommit) {
		return new OTNodeImpl<>(algorithms, commitToObject, objectToCommit);
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTNodeImpl<K, D> create(OTAlgorithms<K, D> algorithms) {
		return new OTNodeImpl<K, D>(algorithms, commit -> commit, object -> (OTCommit<K, D>) object);
	}

	@Override
	public Promise<Object> createCommit(K parent, List<? extends D> diffs, long level) {
		return repository.createCommit(parent, diffs, level)
				.thenApply(commitToObject);
	}

	@Override
	public Promise<K> push(Object commitData) {
		OTCommit<K, D> otCommit = objectToCommit.apply(commitData);
		return repository.push(otCommit)
				.thenApply($ -> otCommit.getId());
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
								concat(checkoutData.getDiffs(), fetchData.getDiffs())
						))
				)
				.whenComplete(toLogger(logger, thisMethod()));
	}

	@Override
	public Promise<FetchData<K, D>> fetch(K currentCommitId) {
		return algorithms.merge()
				.thenApply(Collections::singleton)
				.thenCompose(heads -> algorithms.findParent(
						heads,
						DiffsReducer.toList(),
						commit -> Promise.of(commit.getId().equals(currentCommitId))
				))
				.thenApply(findResult -> new FetchData<>(
						findResult.getChild(),
						findResult.getChildLevel(),
						findResult.getAccumulatedDiffs()
				))
				.whenComplete(toLogger(logger, thisMethod(), currentCommitId));
	}

}
