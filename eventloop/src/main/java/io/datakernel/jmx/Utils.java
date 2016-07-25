/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.jmx;

import javax.management.openmbean.OpenType;
import java.util.*;

final class Utils {

	private Utils() {}

	public static <T> List<T> filterNulls(List<T> src) {
		List<T> out = new ArrayList<>();
		for (T item : src) {
			if (item != null) {
				out.add(item);
			}
		}
		return out;
	}

	public static Map<String, OpenType<?>> createMapWithOneEntry(String key, OpenType<?> openType) {
		Map<String, OpenType<?>> map = new HashMap<>();
		map.put(key, openType);
		return map;
	}

	public static <T> Iterable<T> concat(final Iterable<? extends Iterable<T>> iterables) {
		return new Iterable<T>() {
			@Override
			public Iterator<T> iterator() {
				return new Iterator<T>() {
					private final Iterator<? extends Iterable<T>> iterablesIterator = iterables.iterator();
					// we assume that iterable.iterator() cannot return null
					private Iterator<T> currentIterator =
							iterablesIterator.hasNext() ? iterablesIterator.next().iterator() : null;

					@Override
					public boolean hasNext() {
						adjustCurrentIterator();
						return currentIterator != null;
					}

					@Override
					public T next() {
						adjustCurrentIterator();
						if (currentIterator == null) {
							throw new NoSuchElementException();
						}
						return currentIterator.next();
					}

					private void adjustCurrentIterator() {
						if (currentIterator == null || currentIterator.hasNext()) {
							return;
						}

						// find next non-empty iterator
						while (true) {
							// we assume that iterable.iterator() cannot return null
							currentIterator =
									iterablesIterator.hasNext() ? iterablesIterator.next().iterator() : null;
							if (currentIterator == null || currentIterator.hasNext()) {
								// currentIterator == null means that we've got to the end
								return;
							}
						}
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException("remove");
					}
				};
			}
		};
	}
}
