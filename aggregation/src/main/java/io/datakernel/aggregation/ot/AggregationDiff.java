package io.datakernel.aggregation.ot;

import io.datakernel.aggregation.AggregationChunk;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public final class AggregationDiff {
	private final Set<AggregationChunk> addedChunks;
	private final Set<AggregationChunk> removedChunks;

	private AggregationDiff(Set<AggregationChunk> addedChunks, Set<AggregationChunk> removedChunks) {
		this.addedChunks = addedChunks;
		this.removedChunks = removedChunks;
	}

	public static AggregationDiff of(Set<AggregationChunk> addedChunks, Set<AggregationChunk> removedChunks) {
		return new AggregationDiff(addedChunks, removedChunks);
	}

	public static AggregationDiff ofCommit(Set<AggregationChunk> addedChunks) {
		return new AggregationDiff(addedChunks, Collections.<AggregationChunk>emptySet());
	}

	public static AggregationDiff empty() {
		return new AggregationDiff(Collections.<AggregationChunk>emptySet(), Collections.<AggregationChunk>emptySet());
	}

	public Set<AggregationChunk> getAddedChunks() {
		return addedChunks;
	}

	public Set<AggregationChunk> getRemovedChunks() {
		return removedChunks;
	}

	public AggregationDiff inverse() {
		return new AggregationDiff(removedChunks, addedChunks);
	}

	public boolean isEmpty() {
		return addedChunks.isEmpty() && removedChunks.isEmpty();
	}

	public static AggregationDiff simplify(AggregationDiff commit1, AggregationDiff commit2) {
		Set<AggregationChunk> addedChunks = new LinkedHashSet<>(commit1.addedChunks);
		addedChunks.removeAll(commit2.removedChunks);
		addedChunks.addAll(commit2.addedChunks);

		Set<AggregationChunk> removedChunks = new LinkedHashSet<>(commit1.removedChunks);
		Set<AggregationChunk> removedChunks2 = new LinkedHashSet<>(commit2.removedChunks);
		removedChunks2.removeAll(commit1.addedChunks);
		removedChunks.addAll(removedChunks2);

		return of(addedChunks, removedChunks);
	}

}
