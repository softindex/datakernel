package io.datakernel.crdt.primitives;

import io.datakernel.serializer.AbstractBinarySerializer;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.util.BinaryInput;
import io.datakernel.serializer.util.BinaryOutput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class GMap<K, V extends CrdtMergable<V>> implements Map<K, V>, CrdtMergable<GMap<K, V>> {

	private final Map<K, V> map;

	private GMap(Map<K, V> map) {
		this.map = map;
	}

	public GMap() {
		this(new HashMap<>());
	}

	@Override
	public GMap<K, V> merge(GMap<K, V> other) {
		HashMap<K, V> newMap = new HashMap<>(map);
		other.map.forEach((k, v) -> newMap.merge(k, v, CrdtMergable::merge));
		return new GMap<>(newMap);
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	public V get(Object key) {
		return map.get(key);
	}

	@Nullable
	@Override
	public V put(K key, V value) {
		return map.merge(key, value, CrdtMergable::merge);
	}

	@Override
	public V remove(Object key) {
		throw new UnsupportedOperationException("GMap is a grow-only map");
	}

	@Override
	public void putAll(@NotNull Map<? extends K, ? extends V> m) {
		m.forEach(this::put);
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("GMap is a grow-only map");
	}

	@NotNull
	@Override
	public Set<K> keySet() {
		throw new UnsupportedOperationException("GMap#keySet is not implemented yet");
	}

	@NotNull
	@Override
	public Collection<V> values() {
		throw new UnsupportedOperationException("GMap#values is not implemented yet");
	}

	@NotNull
	@Override
	public Set<Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException("GMap#entrySet is not implemented yet");
	}

	public static class Serializer<K, V extends CrdtMergable<V>> extends AbstractBinarySerializer<GMap<K, V>> {
		private final BinarySerializer<K> keySerializer;
		private final BinarySerializer<V> valueSerializer;

		public Serializer(BinarySerializer<K> keySerializer, BinarySerializer<V> valueSerializer) {
			this.keySerializer = keySerializer;
			this.valueSerializer = valueSerializer;
		}

		@Override
		public void encode(BinaryOutput out, GMap<K, V> item) {
			out.writeVarInt(item.map.size());
			for (Entry<K, V> entry : item.map.entrySet()) {
				keySerializer.encode(out, entry.getKey());
				valueSerializer.encode(out, entry.getValue());
			}
		}

		@Override
		public GMap<K, V> decode(BinaryInput in) {
			int size = in.readVarInt();
			Map<K, V> map = new HashMap<>(size);
			for (int i = 0; i < size; i++) {
				map.put(keySerializer.decode(in), valueSerializer.decode(in));
			}
			return new GMap<>(map);
		}
	}
}
