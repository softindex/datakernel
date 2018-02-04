package io.datakernel.async;

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
		return resultOf(Collections.singletonList(value1));
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

	IndexedCollector<Object, Object[], Object[]> TO_ARRAY = new IndexedCollector<Object, Object[], Object[]>() {
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
		public Object[] finish(Object[] accumulator) {
			return accumulator;
		}

		@Override
		public Object[] resultOf() {
			return new Object[0];
		}

		@Override
		public Object[] resultOf(Object value1) {
			return new Object[]{value1};
		}

		@Override
		public Object[] resultOf(Object value1, Object value2) {
			return new Object[]{value1, value2};
		}

		@Override
		public Object[] resultOf(List<?> values) {
			return values.toArray(new Object[values.size()]);
		}
	};

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
			return Collections.singletonList(value1);
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
	static <T> IndexedCollector<T, T[], T[]> toArray() {
		return (IndexedCollector) TO_ARRAY;
	}
}
