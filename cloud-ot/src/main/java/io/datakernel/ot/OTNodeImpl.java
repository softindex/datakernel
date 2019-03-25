package io.datakernel.ot;

import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.util.ref.Ref;
import io.datakernel.util.ref.RefInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.ot.DiffsReducer.toSquashedList;
import static io.datakernel.ot.OTAlgorithms.*;
import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static java.util.Collections.singleton;

public final class OTNodeImpl<K, D, C> implements OTNode<K, D, C> {
	private static final Logger logger = LoggerFactory.getLogger(OTNodeImpl.class);

	private final OTSystem<D> otSystem;
	private final OTRepository<K, D> repository;
	private final Function<OTCommit<K, D>, C> commitToObject;
	private final Function<C, OTCommit<K, D>> objectToCommit;

	private OTNodeImpl(OTRepository<K, D> repository, OTSystem<D> otSystem, Function<OTCommit<K, D>, C> commitToObject,
			Function<C, OTCommit<K, D>> objectToCommit) {
		this.otSystem = otSystem;
		this.repository = repository;
		this.commitToObject = commitToObject;
		this.objectToCommit = objectToCommit;
	}

	public static <K, D, C> OTNodeImpl<K, D, C> create(OTRepository<K, D> repository, OTSystem<D> otSystem,
			Function<OTCommit<K, D>, C> commitToObject, Function<C, OTCommit<K, D>> objectToCommit) {
		return new OTNodeImpl<>(repository, otSystem, commitToObject, objectToCommit);
	}

	public static <K, D> OTNodeImpl<K, D, OTCommit<K, D>> create(OTRepository<K, D> repository, OTSystem<D> otSystem) {
		return new OTNodeImpl<>(repository, otSystem, commit -> commit, object -> object);
	}

	public OTRepository<K, D> getRepository() {
		return repository;
	}

	@Override
	public Promise<C> createCommit(K parent, List<? extends D> diffs, long level) {
		return repository.loadCommit(parent)
				.then(parentCommit ->
						repository.createCommit(parentCommit.getEpoch(), parent, diffs, level)
								.map(commitToObject)
								.whenComplete(toLogger(logger, thisMethod(), parent, diffs, level)));
	}

	@Override
	public Promise<FetchData<K, D>> push(C commit) {
		OTCommit<K, D> otCommit = objectToCommit.apply(commit);
		return repository.push(otCommit)
				.then($ -> repository.getHeads())
				.then(initalHeads -> excludeParents(repository, otSystem, union(initalHeads, singleton(otCommit.getId())))
						.then(heads -> merge(repository, otSystem, heads))
						.then(mergeHead -> {
							Set<K> mergeHeadSet = singleton(mergeHead);
							return repository.updateHeads(mergeHeadSet, difference(initalHeads, mergeHeadSet))
									.then($ -> doFetch(mergeHeadSet, otCommit.getId()));
						}))
				.whenComplete(toLogger(logger, thisMethod(), commit));
	}

	@Override
	public Promise<FetchData<K, D>> checkout() {
		RefInt epoch = new RefInt(0);
		Ref<List<D>> cachedSnapshot = new Ref<>();
		return repository.getHeads()
				.then(heads -> findParent(
						repository,
						otSystem,
						heads,
						DiffsReducer.toList(),
						commit -> repository.loadSnapshot(commit.getId())
								.map(maybeSnapshot -> (cachedSnapshot.value = maybeSnapshot.orElse(null)) != null)))
				.whenResult(findResult -> epoch.set(findResult.getEpoch()))
				.then(findResult -> Promise.of(
						new FetchData<>(
								findResult.getChild(),
								findResult.getChildLevel(),
								concat(cachedSnapshot.value, findResult.getAccumulatedDiffs()))))
				.then(checkoutData -> fetch(checkoutData.getCommitId())
						.map(fetchData -> new FetchData<>(
								fetchData.getCommitId(),
								fetchData.getLevel(),
								otSystem.squash(concat(checkoutData.getDiffs(), fetchData.getDiffs()))
						))
				)
				.whenComplete(toLogger(logger, thisMethod()));
	}

	@Override
	public Promise<FetchData<K, D>> fetch(K currentCommitId) {
		return repository.getHeads()
				.then(heads -> doFetch(heads, currentCommitId))
				.whenComplete(toLogger(logger, thisMethod(), currentCommitId));
	}

	@Override
	public Promise<FetchData<K, D>> poll(K currentCommitId) {
		return Promises.until(PollSanitizer.create(repository.pollHeads()),
				AsyncPredicate.of(polledHeads -> !polledHeads.contains(currentCommitId)))
				.then(heads -> doFetch(heads, currentCommitId));
	}

	private Promise<FetchData<K, D>> doFetch(Set<K> heads, K currentCommitId) {
		return findParent(
				repository,
				otSystem,
				heads,
				toSquashedList(otSystem),
				AsyncPredicate.of(commit -> commit.getId().equals(currentCommitId)))
				.map(findResult -> new FetchData<>(
						findResult.getChild(),
						findResult.getChildLevel(),
						otSystem.squash(findResult.getAccumulatedDiffs())
				))
				.whenComplete(toLogger(logger, thisMethod(), currentCommitId));
	}
}
