package io.datakernel.ot;

import io.datakernel.async.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;

public interface OTRepository<K, D> extends OTCommitFactory<K, D> {
	Promise<Void> push(Collection<OTCommit<K, D>> commits);

	default Promise<Void> push(OTCommit<K, D> commit) {
		return push(singletonList(commit));
	}

	Promise<Void> updateHeads(Set<K> newHeads, Set<K> excludedHeads);

	default Promise<Void> pushAndUpdateHead(OTCommit<K, D> commit) {
		return push(commit)
				.then($ -> updateHeads(singleton(commit.getId()), commit.getParentIds()));
	}

	default Promise<Void> pushAndUpdateHeads(Collection<OTCommit<K, D>> commits) {
		Set<K> parents = commits.stream()
				.flatMap(commit -> commit.getParentIds().stream())
				.collect(toSet());
		Set<K> heads = commits.stream()
				.map(OTCommit::getId)
				.filter(commit -> !parents.contains(commit)).collect(toSet());
		return push(commits)
				.then($ -> updateHeads(heads, parents));
	}

	@NotNull
	Promise<Set<K>> getHeads();

	@NotNull
	default Promise<Set<K>> pollHeads(Set<K> heads) {
		return getHeads();
	}

	Promise<OTCommit<K, D>> loadCommit(K revisionId);

	default Promise<Boolean> hasSnapshot(K revisionId) {
		return loadSnapshot(revisionId).map(Optional::isPresent);
	}

	Promise<Optional<List<D>>> loadSnapshot(K revisionId);

	Promise<Void> saveSnapshot(K revisionId, List<D> diffs);

}
