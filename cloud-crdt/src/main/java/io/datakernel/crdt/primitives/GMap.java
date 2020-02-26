/*
 * Copyright (C) 2015-2020 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.crdt.primitives;

import io.datakernel.crdt.Crdt;
import io.datakernel.serializer.BinaryInput;
import io.datakernel.serializer.BinaryOutput;
import io.datakernel.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public final class GMap<K, V extends Crdt<V>> implements Map<K, V>, Crdt<GMap<K, V>> {
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
		other.map.forEach((k, v) -> newMap.merge(k, v, Crdt::merge));
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
		return map.merge(key, value, Crdt::merge);
	}

	@Override
	public V remove(Object key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(@NotNull Map<? extends K, ? extends V> m) {
		m.forEach(this::put);
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@NotNull
	@Override
	public Set<K> keySet() {
		return Collections.unmodifiableSet(map.keySet());
	}

	@NotNull
	@Override
	public Collection<V> values() {
		return Collections.unmodifiableCollection(map.values());
	}

	@NotNull
	@Override
	public Set<Entry<K, V>> entrySet() {
		Set<Entry<K, V>> peer = map.entrySet();
		return new Set<Entry<K, V>>() {
			@Override
			public int size() {
				return peer.size();
			}

			@Override
			public boolean isEmpty() {
				return peer.isEmpty();
			}

			@Override
			public boolean contains(Object o) {
				return peer.contains(o);
			}

			@NotNull
			@Override
			public Iterator<Entry<K, V>> iterator() {
				Iterator<Entry<K, V>> iterator = peer.iterator();
				return new Iterator<Entry<K, V>>() {
					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					@Override
					public Entry<K, V> next() {
						return new AbstractMap.SimpleImmutableEntry<>(iterator.next());
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}

					@Override
					public void forEachRemaining(Consumer<? super Entry<K, V>> action) {
						iterator.forEachRemaining(action);
					}
				};
			}

			@NotNull
			@Override
			public Object[] toArray() {
				return peer.toArray();
			}

			@SuppressWarnings("SuspiciousToArrayCall")
			@NotNull
			@Override
			public <T> T[] toArray(@NotNull T[] a) {
				return peer.toArray(a);
			}

			@Override
			public boolean add(Entry<K, V> kvEntry) {
				return peer.add(kvEntry);
			}

			@Override
			public boolean remove(Object o) {
				return peer.remove(o);
			}

			@Override
			public boolean containsAll(@NotNull Collection<?> c) {
				return peer.containsAll(c);
			}

			@Override
			public boolean addAll(@NotNull Collection<? extends Entry<K, V>> c) {
				return peer.addAll(c);
			}

			@Override
			public boolean retainAll(@NotNull Collection<?> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean removeAll(@NotNull Collection<?> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void clear() {
				throw new UnsupportedOperationException();
			}

			@Override
			public Spliterator<Entry<K, V>> spliterator() {
				return peer.spliterator();
			}

			@Override
			public boolean removeIf(Predicate<? super Entry<K, V>> filter) {
				throw new UnsupportedOperationException();
			}

			@Override
			public Stream<Entry<K, V>> stream() {
				return peer.stream();
			}

			@Override
			public Stream<Entry<K, V>> parallelStream() {
				return peer.parallelStream();
			}

			@Override
			public void forEach(Consumer<? super Entry<K, V>> action) {
				peer.forEach(action);
			}
		};
	}

	public static class Serializer<K, V extends Crdt<V>> implements BinarySerializer<GMap<K, V>> {
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
