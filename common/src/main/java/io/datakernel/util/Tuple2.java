package io.datakernel.util;

import java.util.Objects;

public final class Tuple2<T1, T2> {
	private final T1 value1;
	private final T2 value2;

	public Tuple2(T1 value1, T2 value2) {
		this.value1 = value1;
		this.value2 = value2;
	}

	public T1 getValue1() {
		return value1;
	}

	public T2 getValue2() {
		return value2;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;
		return Objects.equals(value1, tuple2.value1) &&
				Objects.equals(value2, tuple2.value2);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value1, value2);
	}
}
