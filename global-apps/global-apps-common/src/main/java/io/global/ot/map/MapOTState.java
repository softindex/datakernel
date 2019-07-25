package io.global.ot.map;

import io.datakernel.ot.OTState;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MapOTState<K, V> implements OTState<MapOperation<K, V>> {
	private final Map<K, V> map = new HashMap<>();

	@Override
	public void init() {
		map.clear();
	}

	@Override
	public void apply(MapOperation<K, V> mapOperation) {
		Map<K, SetValue<V>> operations = mapOperation.getOperations();
		operations.forEach((key, op) -> {
			V next = op.getNext();
			if (next == null) {
				map.remove(key);
			} else {
				map.put(key, next);
			}
		});
	}

	public Map<K, V> getMap() {
		return Collections.unmodifiableMap(map);
	}
}
