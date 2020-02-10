package io.datakernel.ot;

import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.datakernel.ot.GraphReducer.Result.completePromise;
import static io.datakernel.ot.GraphReducer.Result.resumePromise;
import static java.util.Collections.singletonMap;

public abstract class AbstractGraphReducer<K, D, A, R> implements GraphReducer<K, D, R> {
	private final DiffsReducer<A, D> diffsReducer;
	private final Map<K, Map<K, A>> accumulators = new HashMap<>();
	private final Map<K, OTCommit<K, D>> headCommits = new HashMap<>();

	protected AbstractGraphReducer(DiffsReducer<A, D> diffsReducer) {
		this.diffsReducer = diffsReducer;
	}

	@Override
	public void onStart(@NotNull Collection<OTCommit<K, D>> queue) {
		for (OTCommit<K, D> headCommit : queue) {
			this.headCommits.put(headCommit.getId(), headCommit);
			this.accumulators.put(headCommit.getId(), new HashMap<>(singletonMap(headCommit.getId(), diffsReducer.initialValue())));
		}
	}

	@NotNull
	protected abstract Promise<Optional<R>> tryGetResult(OTCommit<K, D> commit, Map<K, Map<K, A>> accumulators,
			Map<K, OTCommit<K, D>> headCommits);

	@NotNull
	@Override
	public final Promise<Result<R>> onCommit(@NotNull OTCommit<K, D> commit) {
		return tryGetResult(commit, accumulators, headCommits)
				.then(maybeResult -> {
					if (maybeResult.isPresent()) {
						return completePromise(maybeResult.get());
					}

					Map<K, A> toHeads = accumulators.remove(commit.getId());
					for (K parent : commit.getParents().keySet()) {
						Map<K, A> parentToHeads = accumulators.computeIfAbsent(parent, $ -> new HashMap<>());
						for (K head : toHeads.keySet()) {
							A newAccumulatedDiffs = diffsReducer.accumulate(toHeads.get(head), commit.getParents().get(parent));
							A existingAccumulatedDiffs = parentToHeads.get(head);
							A combinedAccumulatedDiffs = existingAccumulatedDiffs == null ?
									newAccumulatedDiffs :
									diffsReducer.combine(existingAccumulatedDiffs, newAccumulatedDiffs);
							parentToHeads.put(head, combinedAccumulatedDiffs);
						}
					}
					return resumePromise();
				});
	}
}
