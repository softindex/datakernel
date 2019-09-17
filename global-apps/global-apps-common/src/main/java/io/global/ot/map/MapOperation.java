package io.global.ot.map;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonMap;

public final class MapOperation<K, V> {
	private final Map<K, SetValue<V>> operations;

	private MapOperation(Map<K, SetValue<V>> operations) {
		this.operations = operations;
	}

	public static <K, V> MapOperation<K, V> of(Map<K, SetValue<V>> operations) {
		return new MapOperation<>(operations);
	}

	public static <K, V> MapOperation<K, V> forKey(K key, SetValue<V> operation) {
		return new MapOperation<>(singletonMap(key, operation));
	}

	public Map<K, SetValue<V>> getOperations() {
		return Collections.unmodifiableMap(operations);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		MapOperation that = (MapOperation) o;

		if (!operations.equals(that.operations)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return operations.hashCode();
	}

}
