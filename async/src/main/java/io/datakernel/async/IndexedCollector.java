package io.datakernel.async;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collector;

public interface IndexedCollector<T, A, R> {
	A accumulator(int stages);

	void accumulate(A accumulator, int stageIndex, T stageResult);

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
			public A accumulator(int stages) {
				return collector.supplier().get();
			}

			@Override
			public void accumulate(A accumulator, int stageIndex, T stageResult) {
				this.accumulator.accept(accumulator, stageResult);
			}

			@Override
			public R finish(A accumulator) {
				return collector.finisher().apply(accumulator);
			}
		};
	}

	IndexedCollector<Object, Object[], List<Object>> TO_LIST = new IndexedCollector<Object, Object[], List<Object>>() {
		@Override
		public Object[] accumulator(int stages) {
			return new Object[stages];
		}

		@Override
		public void accumulate(Object[] accumulator, int stageIndex, Object stageResult) {
			accumulator[stageIndex] = stageResult;
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
		return (IndexedCollector) new IndexedCollector<Object, Object[], Object[]>() {

			@Override
			public T[] accumulator(int stages) {
				return (T[]) Array.newInstance(type, stages);
			}

			@Override
			public void accumulate(Object[] accumulator, int stageIndex, Object stageResult) {
				accumulator[stageIndex] = stageResult;
			}

			@SuppressWarnings("unchecked")
			@Override
			public T[] finish(Object[] accumulator) {
				return (T[]) accumulator;
			}

			@Override
			public T[] resultOf() {
				return (T[]) Array.newInstance(type, 0);
			}

			@Override
			public T[] resultOf(Object value1) {
				T[] array = (T[]) Array.newInstance(type, 1);
				array[0] = (T) value1;
				return array;
			}

			@Override
			public T[] resultOf(Object value1, Object value2) {
				T[] array = (T[]) Array.newInstance(type, 2);
				array[0] = (T) value1;
				array[1] = (T) value2;
				return array;
			}

			@Override
			public T[] resultOf(List<?> values) {
				return (T[]) values.toArray();
			}
		};
	}
}
