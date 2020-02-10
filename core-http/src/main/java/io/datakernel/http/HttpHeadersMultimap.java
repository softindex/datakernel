package io.datakernel.http;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

final class HttpHeadersMultimap<K, V> {
	Object[] kvPairs = new Object[8];
	int size;

	@Contract(pure = true)
	public int size() {
		return size;
	}

	public void add(@NotNull K key, @NotNull V value) {
		if (size++ > kvPairs.length / 4) {
			resize();
		}
		// those -2's below are ok - first -1 is to get the modulo mask
		// and second -1 is so that mask also floors the number to even
		// (because we have flat array of pairs)
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

	@Nullable
	@Contract(pure = true)
	@SuppressWarnings("unchecked")
	public V get(@NotNull K key) {
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

	public final Collection<Map.Entry<K, V>> getEntries() {
		return new AbstractCollection<Map.Entry<K, V>>() {
			@Override
			public int size() {
				return size;
			}

			@NotNull
			@Override
			public Iterator<Map.Entry<K, V>> iterator() {
				return new Iterator<Map.Entry<K, V>>() {
					int i = 0;
					@Nullable K k;
					@Nullable V v;

					{ advance();}

					@SuppressWarnings("unchecked")
					private void advance() {
						for (; i < kvPairs.length; i += 2) {
							K k = (K) kvPairs[i];
							if (k != null) {
								this.k = k;
								this.v = (V) kvPairs[i + 1];
								i += 2;
								return;
							}
						}
						this.k = null;
						this.v = null;
					}

					@Override
					public boolean hasNext() {
						return this.k != null;
					}

					@Override
					public Map.Entry<K, V> next() {
						if (k == null)
							throw new NoSuchElementException();
						Map.Entry<K, V> entry = new AbstractMap.SimpleImmutableEntry<>(this.k, this.v);
						advance();
						return entry;
					}
				};
			}
		};
	}

}
