package io.datakernel.cube.ot;

import io.datakernel.aggregation.ot.AggregationDiff;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
		return new CubeDiff(Collections.<String, AggregationDiff>emptyMap());
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
}
