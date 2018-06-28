package io.datakernel.cube.ot;

import io.datakernel.aggregation.ot.AggregationDiff;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;

public class CubeDiff {
	private final Map<String, AggregationDiff> diffs;

	private CubeDiff(Map<String, AggregationDiff> diffs) {
		this.diffs = diffs;
	}

	public static CubeDiff of(Map<String, AggregationDiff> aggregationOps) {
		Map<String, AggregationDiff> map = new HashMap<>();
		for (Map.Entry<String, AggregationDiff> entry : aggregationOps.entrySet()) {
			AggregationDiff value = entry.getValue();
			if (!value.isEmpty()) {
				map.put(entry.getKey(), value);
			}
		}
		return new CubeDiff(map);
	}


	public Set<String> keySet() {
		return diffs.keySet();
	}

	public AggregationDiff get(String id) {
		return diffs.get(id);
	}

	public static CubeDiff empty() {
		return new CubeDiff(emptyMap());
	}

	public CubeDiff inverse() {
		Map<String, AggregationDiff> map = new HashMap<>();
		for (Map.Entry<String, AggregationDiff> entry : diffs.entrySet()) {
			String key = entry.getKey();
			AggregationDiff value = entry.getValue();
			map.put(key, value.inverse());
		}
		return new CubeDiff(map);
	}

	public boolean isEmpty() {
		return diffs.isEmpty();
	}

	public <C> Stream<C> addedChunks() {
		return diffs.values().stream()
				.flatMap(aggregationDiff -> aggregationDiff.getAddedChunks().stream())
				.map(aggregationChunk -> (C) aggregationChunk.getChunkId());
	}

	@Override
	public String toString() {
		return "{diffs:" + diffs.size() + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		CubeDiff cubeDiff = (CubeDiff) o;

		return diffs != null ? diffs.equals(cubeDiff.diffs) : cubeDiff.diffs == null;
	}

	@Override
	public int hashCode() {
		return diffs != null ? diffs.hashCode() : 0;
	}
}
