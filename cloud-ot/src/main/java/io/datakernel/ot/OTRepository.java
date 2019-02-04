package io.datakernel.ot;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.async.SettablePromise;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.lang.Math.min;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public interface OTRepository<K, D> extends OTCommitFactory<K, D> {
	default Promise<Void> push(Collection<OTCommit<K, D>> commits) {
		return Promises.sequence(commits.stream()
				.sorted(comparingLong(OTCommit::getLevel))
				.map(commit ->
						() -> push(commit)));
	}

	default Promise<Void> push(OTCommit<K, D> commit) {
		return push(singletonList(commit));
	}

	Promise<Set<K>> getHeads();

	Promise<OTCommit<K, D>> loadCommit(K revisionId);

	default Promise<Boolean> hasSnapshot(K revisionId) {
		return loadSnapshot(revisionId).thenApply(Optional::isPresent);
	}

	Promise<Optional<List<D>>> loadSnapshot(K revisionId);

	Promise<Void> saveSnapshot(K revisionId, List<D> diffs);

	static <R, K, D> OTRepository<K, D> compound(OTCommitFactory<K, D> commitFactory,
												 Map<R, OTRepository<K, D>> repositories,
												 Function<K, List<R>> readList,
												 Function<K, List<R>> writeList,
												 int writeRedundancy) {
		return new OTRepository<K, D>() {

			private Promise<Void> doCall(Stream<? extends AsyncSupplier<?>> callables,
										 int minSuccesses) {
				List<? extends AsyncSupplier<?>> list = callables.collect(toList());
				int minSuccessesFinal = min(minSuccesses, list.size());
				if (minSuccessesFinal == 0) {
					list.forEach(AsyncSupplier::get);
					return Promise.complete();
				}
				SettablePromise<Void> result = new SettablePromise<>();
				int[] successes = {0};
				int[] completed = {0};
				for (AsyncSupplier<?> callable : list) {
					callable.get().whenComplete(($, e) -> {
						if (e == null) {
							if (++successes[0] == minSuccessesFinal) {
								result.set(null);
							}
						}
						if (++completed[0] == list.size()) {
							if (!result.isComplete()) {
								result.setException(new IOException());
							}
						}
					});
				}
				return result;
			}

			@Override
			public Promise<Void> push(OTCommit<K, D> commit) {
				return doCall(
						writeList.apply(commit.getId()).stream()
								.map(repositories::get)
								.map(repository -> AsyncSupplier.cast(() ->
										repository.push(commit))),
						writeRedundancy);
			}

			@Override
			public Promise<Set<K>> getHeads() {
				return Promises.toList(repositories.values().stream()
						.map(OTRepository::getHeads)
						.map(Promise::toTry))
						.thenApply(list -> list.stream().flatMap(t -> t.getOr(emptySet()).stream()).collect(toSet()))
						.thenCompose(result -> !result.isEmpty() ?
								Promise.of(result) :
								Promise.ofException(new IOException()));
			}

			@Override
			public Promise<OTCommit<K, D>> loadCommit(K revisionId) {
				return Promises.firstSuccessful(
						readList.apply(revisionId).stream()
								.map(repositories::get)
								.map(repository -> AsyncSupplier.cast(() ->
										repository.loadCommit(revisionId))));
			}

			@Override
			public Promise<Optional<List<D>>> loadSnapshot(K revisionId) {
				return Promises.firstSuccessful(
						readList.apply(revisionId).stream()
								.map(repositories::get)
								.map(repository -> AsyncSupplier.cast(() ->
										repository.loadSnapshot(revisionId))));
			}

			@Override
			public Promise<Void> saveSnapshot(K revisionId, List<D> diffs) {
				return doCall(
						writeList.apply(revisionId).stream()
								.map(repositories::get)
								.map(repository -> AsyncSupplier.cast(() ->
										repository.saveSnapshot(revisionId, diffs))),
						writeRedundancy);
			}

			@Override
			public Promise<OTCommit<K, D>> createCommit(Map<K, ? extends List<? extends D>> parentDiffs, long level) {
				return commitFactory.createCommit(parentDiffs, level);
			}
		};
	}
}
