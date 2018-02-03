package io.datakernel.async;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public interface StagesCollector<T, A, R> {
	A accumulator(int stages);

	void accumulate(A accumulator, int stageIndex, T stageResult);

	R finish(A accumulator);

	default R of() {
		return of(Collections.emptyList());
	}

	default R of(T value1) {
		return of(Collections.singletonList(value1));
	}

	default R of(T value1, T value2) {
		return of(Arrays.asList(value1, value2));
	}

	default R of(T value1, T value2, T value3) {
		return of(Arrays.asList(value1, value2, value3));
	}

	default R of(List<? extends T> values) {
		A accumulator = accumulator(values.size());
		for (int i = 0; i < values.size(); i++) {
			accumulate(accumulator, i, values.get(i));
		}
		return finish(accumulator);
	}

	StagesCollector<Object, Object[], Object[]> TO_ARRAY = new StagesCollector<Object, Object[], Object[]>() {
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
		public Object[] of() {
			return new Object[0];
		}

		@Override
		public Object[] of(Object value1) {
			return new Object[]{value1};
		}

		@Override
		public Object[] of(Object value1, Object value2) {
			return new Object[]{value1, value2};
		}

		@Override
		public Object[] of(Object value1, Object value2, Object value3) {
			return new Object[]{value1, value2, value3};
		}

		@Override
		public Object[] of(List<?> values) {
			return values.toArray(new Object[values.size()]);
		}
	};

	StagesCollector<Object, Object[], List<Object>> TO_LIST = new StagesCollector<Object, Object[], List<Object>>() {
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
		public List<Object> of() {
			return Collections.emptyList();
		}

		@Override
		public List<Object> of(Object value1) {
			return Collections.singletonList(value1);
		}

		@Override
		public List<Object> of(Object value1, Object value2) {
			return Arrays.asList(value1, value2);
		}

		@Override
		public List<Object> of(Object value1, Object value2, Object value3) {
			return Arrays.asList(value1, value2, value3);
		}

		@SuppressWarnings("unchecked")
		@Override
		public List<Object> of(List<?> values) {
			return (List<Object>) values;
		}
	};

	@SuppressWarnings("unchecked")
	static <T> StagesCollector<T, Object[], List<T>> toList() {
		return (StagesCollector) TO_LIST;
	}

	@SuppressWarnings("unchecked")
	static <T> StagesCollector<T, T[], T[]> toArray() {
		return (StagesCollector) TO_ARRAY;
	}
}
