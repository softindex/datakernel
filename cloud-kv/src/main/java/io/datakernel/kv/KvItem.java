package io.datakernel.kv;

import io.datakernel.serializer.BinaryInput;
import io.datakernel.serializer.BinaryOutput;
import io.datakernel.serializer.BinarySerializer;
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
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		KvItem<?, ?> kvItem = (KvItem<?, ?>) o;
		return timestamp == kvItem.timestamp && key.equals(kvItem.key) && Objects.equals(value, kvItem.value);
	}

	@Override
	public int hashCode() {
		return 961 * (int) (timestamp ^ timestamp >>> 32) + 31 * key.hashCode() + (value != null ? value.hashCode() : 0);
	}

	public static final class KvItemSerializer<K, V> implements BinarySerializer<KvItem<K, V>> {
		private final BinarySerializer<K> keySerializer;
		private final BinarySerializer<V> valueSerializer;

		public KvItemSerializer(BinarySerializer<K> keySerializer, BinarySerializer<V> valueSerializer) {
			this.keySerializer = keySerializer;
			this.valueSerializer = valueSerializer;
		}

		public BinarySerializer<K> getKeySerializer() {
			return keySerializer;
		}

		public BinarySerializer<V> getValueSerializer() {
			return valueSerializer;
		}

		@Override
		public void encode(BinaryOutput out, KvItem<K, V> item) {
			out.writeLong(item.timestamp);
			keySerializer.encode(out, item.key);
			valueSerializer.encode(out, item.value);
		}

		@Override
		public KvItem<K, V> decode(BinaryInput in) {
			return new KvItem<>(in.readLong(), keySerializer.decode(in), valueSerializer.decode(in));
		}
	}
}
