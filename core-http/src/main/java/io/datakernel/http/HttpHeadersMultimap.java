package io.datakernel.http;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

final class HttpHeadersMultimap<K, V> {
	private Object[] kvPairs = new Object[8];
	private int size;

	public int size() {
		return size;
	}

	public void add(K key, V value) {
		if (size++ > kvPairs.length / 4) {
			resize();
		}
		for (int i = key.hashCode() & (kvPairs.length - 2); ; i = (i + 2) & (kvPairs.length - 2)) {
			if (kvPairs[i] == null) {
				kvPairs[i] = key;
				kvPairs[i + 1] = value;
				return;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void resize() {
		Object[] oldKvPairs = this.kvPairs;
		this.kvPairs = new Object[this.kvPairs.length * 4];
		for (int i = 0; i != oldKvPairs.length; i += 2) {
			K k = (K) oldKvPairs[i];
			if (k != null) {
				V v = (V) oldKvPairs[i + 1];
				add(k, v);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public V get(K key) {
		for (int i = key.hashCode() & (kvPairs.length - 2); ; i = (i + 2) & (kvPairs.length - 2)) {
			K k = (K) kvPairs[i];
			if (k == null) {
				return null;
			}
			if (k.equals(key)) {
				return (V) kvPairs[i + 1];
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void forEach(K key, Consumer<V> valueConsumer) {
		for (int i = key.hashCode() & (kvPairs.length - 2); ; i = (i + 2) & (kvPairs.length - 2)) {
			K k = (K) kvPairs[i];
			if (k == null) {
				return;
			}
			if (k.equals(key)) {
				valueConsumer.accept((V) kvPairs[i + 1]);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void forEach(BiConsumer<K, V> consumer) {
		for (int i = 0; i != this.kvPairs.length; i += 2) {
			K k = (K) kvPairs[i];
			if (k != null) {
				V v = (V) kvPairs[i + 1];
				consumer.accept(k, v);
			}
		}
	}
}
