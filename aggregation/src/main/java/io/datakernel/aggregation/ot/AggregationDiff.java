package io.datakernel.aggregation.ot;

import io.datakernel.aggregation.AggregationChunk;

import java.util.Collections;
import java.util.HashSet;
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

	public static AggregationDiff of(Set<AggregationChunk> addedChunks) {
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

	public static AggregationDiff squash(AggregationDiff commit1, AggregationDiff commit2) {
		Set<AggregationChunk> addedChunks1 = new LinkedHashSet<>(commit1.addedChunks);
		addedChunks1.removeAll(commit2.removedChunks);
		Set<AggregationChunk> addedChunks2 = new LinkedHashSet<>(commit2.addedChunks);
		addedChunks2.removeAll(commit1.removedChunks);
		Set<AggregationChunk> addedChunks = new HashSet<>();
		addedChunks.addAll(addedChunks1);
		addedChunks.addAll(addedChunks2);

		Set<AggregationChunk> removedChunks1 = new LinkedHashSet<>(commit1.removedChunks);
		removedChunks1.removeAll(commit2.addedChunks);
		Set<AggregationChunk> removedChunks2 = new LinkedHashSet<>(commit2.removedChunks);
		removedChunks2.removeAll(commit1.addedChunks);
		Set<AggregationChunk> removedChunks = new HashSet<>();
		removedChunks.addAll(removedChunks1);
		removedChunks.addAll(removedChunks2);

		return of(addedChunks, removedChunks);
	}

	@Override
	public String toString() {
		return "AggregationDiff{" +
				"addedChunks=" + addedChunks +
				", removedChunks=" + removedChunks +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AggregationDiff that = (AggregationDiff) o;

		if (addedChunks != null ? !addedChunks.equals(that.addedChunks) : that.addedChunks != null) return false;
		return removedChunks != null ? removedChunks.equals(that.removedChunks) : that.removedChunks == null;
	}

	@Override
	public int hashCode() {
		int result = addedChunks != null ? addedChunks.hashCode() : 0;
		result = 31 * result + (removedChunks != null ? removedChunks.hashCode() : 0);
		return result;
	}
}
