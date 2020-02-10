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

import io.datakernel.serializer.BinaryInput;
import io.datakernel.serializer.BinaryOutput;
import io.datakernel.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Stream;

public final class TPSet<E> implements Set<E>, CrdtMergable<TPSet<E>> {
	private final GSet<E> adds;
	private final GSet<E> removes;

	private TPSet(GSet<E> adds, GSet<E> removes) {
		this.adds = adds;
		this.removes = removes;
	}

	public TPSet() {
		this(new GSet<>(), new GSet<>());
	}

	@SafeVarargs
	public static <T> TPSet<T> of(T... items) {
		TPSet<T> set = new TPSet<>();
		set.addAll(Arrays.asList(items));
		return set;
	}

	@Override
	public TPSet<E> merge(TPSet<E> other) {
		return new TPSet<>(adds.merge(other.adds), removes.merge(other.removes));
	}

	@Override
	public Stream<E> stream() {
		return adds.stream().filter(item -> !removes.contains(item));
	}

	@Override
	public int size() {
		//noinspection ReplaceInefficientStreamCount
		return (int) stream().count();
	}

	@Override
	public boolean isEmpty() {
		return adds.isEmpty() && removes.isEmpty() || removes.containsAll(adds);
	}

	@Override
	public boolean contains(Object o) {
		return adds.contains(o) && !removes.contains(o);
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

	@SuppressWarnings("SuspiciousToArrayCall")
	@Override
	public <T> T[] toArray(@NotNull T[] a) {
		return stream().toArray($ -> a);
	}

	@Override
	public boolean add(E e) {
		return adds.add(e);
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean remove(Object o) {
		return removes.add((E) o);
	}

	@Override
	public boolean containsAll(@NotNull Collection<?> c) {
		return adds.containsAll(c) && removes.stream().noneMatch(c::contains);
	}

	@Override
	public boolean addAll(@NotNull Collection<? extends E> c) {
		return adds.addAll(c);
	}

	@Override
	public boolean retainAll(@NotNull Collection<?> c) {
		boolean removed = false;
		for (E item : adds) {
			if (!c.contains(item)) {
				removes.add(item);
				removed = true;
			}
		}
		return removed;
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean removeAll(@NotNull Collection<?> c) {
		return removes.addAll((Collection<? extends E>) c);
	}

	@Override
	public void clear() {
		removes.addAll(adds);
	}

	@Override
	public String toString() {
		Set<E> set = new HashSet<>();
		for (E e : adds) {
			if (!removes.contains(e)) {
				set.add(e);
			}
		}
		return set.toString();
	}

	public static class Serializer<T> implements BinarySerializer<TPSet<T>> {
		private final BinarySerializer<GSet<T>> gSetSerializer;

		public Serializer(BinarySerializer<T> valueSerializer) {
			gSetSerializer = new GSet.Serializer<>(valueSerializer);
		}

		@Override
		public void encode(BinaryOutput out, TPSet<T> item) {
			gSetSerializer.encode(out, item.adds);
			gSetSerializer.encode(out, item.removes);
		}

		@Override
		public TPSet<T> decode(BinaryInput in) {
			return new TPSet<>(gSetSerializer.decode(in), gSetSerializer.decode(in));
		}
	}
}
