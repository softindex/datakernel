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

import java.util.*;

public final class GSet<E> implements Set<E> {
	private final Set<E> set;

	public GSet() {
		set = new HashSet<>();
	}

	private GSet(Set<E> newSet) {
		set = newSet;
	}

	@SafeVarargs
	public static <T> GSet<T> of(T... items) {
		GSet<T> set = new GSet<>();
		set.addAll(Arrays.asList(items));
		return set;
	}

	public GSet<E> merge(GSet<E> other) {
		Set<E> newSet = new HashSet<>(set);
		newSet.addAll(other.set);
		return new GSet<>(newSet);
	}

	@Override
	public int size() {
		return set.size();
	}

	@Override
	public boolean isEmpty() {
		return set.isEmpty();
	}

	@Override
	public boolean contains(Object t) {
		return set.contains(t);
	}

	@Override
	public Iterator<E> iterator() {
		return set.iterator();
	}

	@Override
	public Object[] toArray() {
		return set.toArray();
	}

	@Override
	@SuppressWarnings("SuspiciousToArrayCall")
	public <T> T[] toArray(T[] a) {
		return set.toArray(a);
	}

	@Override
	public boolean add(E e) {
		return set.add(e);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return set.containsAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		return set.addAll(c);
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException("GSet is a grow-only set");
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("GSet is a grow-only set");
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("GSet is a grow-only set");
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("GSet is a grow-only set");
	}

	@Override
	public String toString() {
		return set.toString();
	}

	public static class Serializer<T> implements BufferSerializer<GSet<T>> {
		private final BufferSerializer<T> valueSerializer;

		public Serializer(BufferSerializer<T> valueSerializer) {
			this.valueSerializer = valueSerializer;
		}

		@Override
		public void serialize(ByteBuf output, GSet<T> item) {
			output.writeVarInt(item.set.size());
			for (T t : item.set) {
				valueSerializer.serialize(output, t);
			}
		}

		@Override
		public GSet<T> deserialize(ByteBuf input) {
			int size = input.readVarInt();
			Set<T> set = new HashSet<>(size);
			for (int i = 0; i < size; i++) {
				set.add(valueSerializer.deserialize(input));
			}
			return new GSet<>(set);
		}
	}
}
