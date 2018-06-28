package io.datakernel.ot;

import io.datakernel.async.AsyncCallable;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.async.Stages.runSequence;
import static java.lang.Math.min;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public interface OTRemote<K, D> extends OTCommitFactory<K, D> {
	default Stage<Void> push(Collection<OTCommit<K, D>> commits) {
		return runSequence(commits.stream()
				.sorted(comparingLong(OTCommit::getLevel))
				.map(commit -> (AsyncCallable<Void>) () -> this.push(commit)));
	}

	default Stage<Void> push(OTCommit<K, D> commit) {
		return push(singletonList(commit));
	}

	Stage<Set<K>> getHeads();

	Stage<OTCommit<K, D>> loadCommit(K revisionId);

	Stage<List<D>> loadSnapshot(K revisionId);

	Stage<Void> saveSnapshot(K revisionId, List<D> diffs);

	static <R, K, D> OTRemote<K, D> compound(OTCommitFactory<K, D> commitFactory,
			Map<R, OTRemote<K, D>> remotes,
			Function<K, List<R>> readList,
			Function<K, List<R>> writeList,
			int writeRedundancy) {
		return new OTRemote<K, D>() {

			private Stage<Void> doCall(Stream<? extends AsyncCallable<?>> callables,
					int minSuccesses) {
				List<? extends AsyncCallable<?>> list = callables.collect(toList());
				int minSuccessesFinal = min(minSuccesses, list.size());
				if (minSuccessesFinal == 0) {
					list.forEach(AsyncCallable::call);
					return Stage.of(null);
				}
				SettableStage<Void> result = SettableStage.create();
				int[] successes = new int[]{0};
				int[] completed = new int[]{0};
				for (AsyncCallable<?> callable : list) {
					callable.call().whenComplete(($, e) -> {
						if (e == null) {
							if (++successes[0] == minSuccessesFinal) {
								result.set(null);
							}
						}
						if (++completed[0] == list.size()) {
							if (!result.isSet()) {
								result.setException(new IOException());
							}
						}
					});
				}
				return result;
			}

			@Override
			public Stage<Void> push(OTCommit<K, D> commit) {
				return doCall(
						writeList.apply(commit.getId()).stream()
								.map(remotes::get)
								.map(remote -> (AsyncCallable<Void>) () -> remote.push(commit)),
						writeRedundancy);
			}

			@Override
			public Stage<Set<K>> getHeads() {
				return Stages.toList(remotes.values().stream()
						.map(OTRemote::getHeads)
						.map(Stage::toTry))
						.thenApply(list -> list.stream().flatMap(t -> t.getOr(emptySet()).stream()).collect(toSet()))
						.thenCompose(result -> !result.isEmpty() ?
								Stage.of(result) :
								Stage.ofException(new IOException()));
			}

			@Override
			public Stage<OTCommit<K, D>> loadCommit(K revisionId) {
				return Stages.firstSuccessful(
						readList.apply(revisionId).stream()
								.map(remotes::get)
								.map(remote -> (AsyncCallable<OTCommit<K, D>>) () -> remote.loadCommit(revisionId)));
			}

			@Override
			public Stage<List<D>> loadSnapshot(K revisionId) {
				return Stages.firstSuccessful(
						readList.apply(revisionId).stream()
								.map(remotes::get)
								.map(remote -> (AsyncCallable<List<D>>) () -> remote.loadSnapshot(revisionId)));
			}

			@Override
			public Stage<Void> saveSnapshot(K revisionId, List<D> diffs) {
				return doCall(
						writeList.apply(revisionId).stream()
								.map(remotes::get)
								.map(remote -> (AsyncCallable<Void>) () -> remote.saveSnapshot(revisionId, diffs)),
						writeRedundancy);
			}

			@Override
			public Stage<OTCommit<K, D>> createCommit(Map<K, ? extends List<? extends D>> parentDiffs, long level) {
				return commitFactory.createCommit(parentDiffs, level);
			}
		};
	}
}
