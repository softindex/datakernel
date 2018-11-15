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

package io.datakernel.async;

import io.datakernel.annotation.Nullable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collector;

public interface IndexedCollector<T, A, R> {
	A accumulator(int promises);

	void accumulate(A accumulator, int promiseIndex, @Nullable T promiseResult);

	R finish(A accumulator);

	default R resultOf() {
		return resultOf(Collections.emptyList());
	}

	default R resultOf(T value1) {
		//noinspection ArraysAsListWithZeroOrOneArgument - using asList instead of singletonList() to allow mutability
		return resultOf(Arrays.asList(value1));
	}

	default R resultOf(T value1, T value2) {
		return resultOf(Arrays.asList(value1, value2));
	}

	default R resultOf(List<? extends T> values) {
		A accumulator = accumulator(values.size());
		for (int i = 0; i < values.size(); i++) {
			accumulate(accumulator, i, values.get(i));
		}
		return finish(accumulator);
	}

	static <T, A, R> IndexedCollector<T, A, R> ofCollector(Collector<T, A, R> collector) {
		return new IndexedCollector<T, A, R>() {
			final BiConsumer<A, T> accumulator = collector.accumulator();

			@Override
			public A accumulator(int promises) {
				return collector.supplier().get();
			}

			@Override
			public void accumulate(A accumulator, int promiseIndex, T promiseResult) {
				this.accumulator.accept(accumulator, promiseResult);
			}

			@Override
			public R finish(A accumulator) {
				return collector.finisher().apply(accumulator);
			}
		};
	}

	IndexedCollector<Object, Object[], List<Object>> TO_LIST = new IndexedCollector<Object, Object[], List<Object>>() {
		@Override
		public Object[] accumulator(int promises) {
			return new Object[promises];
		}

		@Override
		public void accumulate(Object[] accumulator, int promiseIndex, Object promiseResult) {
			accumulator[promiseIndex] = promiseResult;
		}

		@SuppressWarnings("unchecked")
		@Override
		public List<Object> finish(Object[] accumulator) {
			return Arrays.asList(accumulator);
		}

		@Override
		public List<Object> resultOf() {
			return Collections.emptyList();
		}

		@Override
		public List<Object> resultOf(Object value1) {
			return Arrays.asList(value1);
		}

		@Override
		public List<Object> resultOf(Object value1, Object value2) {
			return Arrays.asList(value1, value2);
		}

		@SuppressWarnings("unchecked")
		@Override
		public List<Object> resultOf(List<?> values) {
			return (List<Object>) values;
		}
	};

	@SuppressWarnings("unchecked")
	static <T> IndexedCollector<T, Object[], List<T>> toList() {
		return (IndexedCollector) TO_LIST;
	}

	@SuppressWarnings("unchecked")
	static <T> IndexedCollector<T, T[], T[]> toArray(Class<T> type) {
		return new IndexedCollector<T, T[], T[]>() {

			@Override
			public T[] accumulator(int promises) {
				return (T[]) Array.newInstance(type, promises);
			}

			@Override
			public void accumulate(T[] accumulator, int promiseIndex, T promiseResult) {
				accumulator[promiseIndex] = promiseResult;
			}

			@SuppressWarnings("unchecked")
			@Override
			public T[] finish(T[] accumulator) {
				return (T[]) accumulator;
			}

			@Override
			public T[] resultOf() {
				return (T[]) Array.newInstance(type, 0);
			}

			@Override
			public T[] resultOf(T value1) {
				T[] array = (T[]) Array.newInstance(type, 1);
				array[0] = (T) value1;
				return array;
			}

			@Override
			public T[] resultOf(T value1, T value2) {
				T[] array = (T[]) Array.newInstance(type, 2);
				array[0] = (T) value1;
				array[1] = (T) value2;
				return array;
			}

			@Override
			public T[] resultOf(List<? extends T> values) {
				return values.toArray(accumulator(0));
			}
		};
	}
}
