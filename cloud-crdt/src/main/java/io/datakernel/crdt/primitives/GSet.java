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

import java.util.*;

public final class GSet<E> implements Set<E>, Crdt<GSet<E>> {
	private final Set<E> set;

	private GSet(Set<E> newSet) {
		set = newSet;
	}

	public GSet() {
		this(new HashSet<>());
	}

	public static <E> GSet<E> fromSet(Set<E> set) {
		return new GSet<>(new HashSet<>(set));
	}

	@SafeVarargs
	public static <T> GSet<T> of(T... items) {
		return new GSet<>(new HashSet<>(Arrays.asList(items)));
	}

	@Override
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
	public <T> T[] toArray(@NotNull T[] a) {
		return set.toArray(a);
	}

	@Override
	public boolean add(E e) {
		return set.add(e);
	}

	@Override
	public boolean containsAll(@NotNull Collection<?> c) {
		return set.containsAll(c);
	}

	@Override
	public boolean addAll(@NotNull Collection<? extends E> c) {
		return set.addAll(c);
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
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
	public String toString() {
		return set.toString();
	}

	public static class Serializer<T> implements BinarySerializer<GSet<T>> {
		private final BinarySerializer<T> valueSerializer;

		public Serializer(BinarySerializer<T> valueSerializer) {
			this.valueSerializer = valueSerializer;
		}

		@Override
		public void encode(BinaryOutput out, GSet<T> item) {
			out.writeVarInt(item.set.size());
			for (T t : item.set) {
				valueSerializer.encode(out, t);
			}
		}

		@Override
		public GSet<T> decode(BinaryInput in) {
			int size = in.readVarInt();
			Set<T> set = new HashSet<>(size);
			for (int i = 0; i < size; i++) {
				set.add(valueSerializer.decode(in));
			}
			return new GSet<>(set);
		}
	}
}
