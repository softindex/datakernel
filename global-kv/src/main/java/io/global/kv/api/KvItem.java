package io.global.kv.api;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public class KvItem<K, V> {
	private final long timestamp;
	private final K key;

	@Nullable
	private final V value;

	public KvItem(long timestamp, K key, @Nullable V value) {
		this.timestamp = timestamp;
		this.key = key;
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public K getKey() {
		return key;
	}

	@Nullable
	public V getValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		KvItem<?, ?> kvItem = (KvItem<?, ?>) o;

		return timestamp == kvItem.timestamp && key.equals(kvItem.key) && Objects.equals(value, kvItem.value);

	}

	@Override
	public int hashCode() {
		return 961 * (int) (timestamp ^ (timestamp >>> 32)) + 31 * key.hashCode() + (value != null ? value.hashCode() : 0);
	}
}
