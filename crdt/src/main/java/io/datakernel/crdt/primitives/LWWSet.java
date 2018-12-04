/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.time.CurrentTimeProvider;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.max;

public final class LWWSet<E> implements Set<E> {
	private final Map<E, Timestamps> set;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private LWWSet(Map<E, Timestamps> set) {
		this.set = set;
	}

	public LWWSet() {
		set = new HashMap<>();
	}

	@SafeVarargs
	public static <T> LWWSet<T> of(T... items) {
		LWWSet<T> set = new LWWSet<>();
		set.addAll(Arrays.asList(items));
		return set;
	}

	public LWWSet<E> merge(LWWSet<E> other) {
		Map<E, Timestamps> newSet = new HashMap<>(set);
		for (Map.Entry<E, Timestamps> entry : other.set.entrySet()) {
			newSet.merge(entry.getKey(), entry.getValue(), (ts1, ts2) -> new Timestamps(max(ts1.added, ts2.added), max(ts1.removed, ts2.removed)));
		}
		return new LWWSet<>(newSet);
	}

	@Override
	public Stream<E> stream() {
		return set.entrySet().stream().filter(e -> e.getValue().exists()).map(Map.Entry::getKey);
	}

	@Override
	public int size() {
		//noinspection ReplaceInefficientStreamCount ikwiad
		return (int) stream().count();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean contains(Object o) {
		//noinspection SuspiciousMethodCalls
		Timestamps timestamps = set.get(o);
		return timestamps != null && timestamps.added >= timestamps.removed;
	}

	@Override
	public Iterator<E> iterator() {
		return stream().iterator();
	}

	@Override
	public Object[] toArray() {
		//noinspection SimplifyStreamApiCallChains
		return stream().toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return stream().toArray($ -> a);
	}

	@Override
	public boolean add(E t) {
		Timestamps timestamps = set.get(t);
		if (timestamps == null) {
			set.put(t, new Timestamps(now.currentTimeMillis(), 0));
			return true;
		}
		boolean notExisted = !timestamps.exists();
		timestamps.added = now.currentTimeMillis();
		return notExisted;
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean remove(Object o) {
		//noinspection SuspiciousMethodCalls
		Timestamps timestamps = set.get(o);
		if (timestamps == null) {
			set.put((E) o, new Timestamps(0, now.currentTimeMillis()));
			return false;
		}
		boolean existed = timestamps.exists();
		timestamps.removed = now.currentTimeMillis();
		return existed;
	}


	@Override
	public boolean containsAll(Collection<?> c) {
		return stream().allMatch(c::contains);
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		boolean added = false;
		for (E e : c) {
			added |= add(e);
		}
		return added;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		boolean removed = false;
		for (E item : this) {
			if (!c.contains(item)) {
				remove(item);
				removed = true;
			}
		}
		return removed;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		boolean removed = false;
		for (Object o : c) {
			removed |= remove(o);
		}
		return removed;
	}

	@Override
	public void clear() {
		forEach(this::remove);
	}

	@Override
	public String toString() {
		return set.entrySet()
				.stream()
				.filter(e -> e.getValue().exists())
				.map(e -> Objects.toString(e.getKey()))
				.collect(Collectors.joining(", ", "[", "]"));
	}

	private static final class Timestamps {
		long added, removed;

		Timestamps(long added, long removed) {
			this.added = added;
			this.removed = removed;
		}

		boolean exists() {
			return added >= removed;
		}
	}

	public static class Serializer<T> implements BufferSerializer<LWWSet<T>> {
		private final BufferSerializer<T> valueSerializer;

		public Serializer(BufferSerializer<T> valueSerializer) {
			this.valueSerializer = valueSerializer;
		}

		@Override
		public void serialize(ByteBuf output, LWWSet<T> item) {
			output.writeVarInt(item.set.size());
			for (Map.Entry<T, Timestamps> entry : item.set.entrySet()) {
				valueSerializer.serialize(output, entry.getKey());
				Timestamps timestamps = entry.getValue();
				output.writeLong(timestamps.added);
				output.writeLong(timestamps.removed);
			}
		}

		@Override
		public LWWSet<T> deserialize(ByteBuf input) {
			int size = input.readVarInt();
			Map<T, Timestamps> set = new HashMap<>(size);
			for (int i = 0; i < size; i++) {
				set.put(valueSerializer.deserialize(input), new Timestamps(input.readLong(), input.readLong()));
			}
			return new LWWSet<>(set);
		}
	}
}