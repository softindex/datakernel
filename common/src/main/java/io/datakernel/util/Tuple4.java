package io.datakernel.util;

import java.util.Objects;

public final class Tuple4<T1, T2, T3, T4> {
	private final T1 value1;
	private final T2 value2;
	private final T3 value3;
	private final T4 value4;

	public Tuple4(T1 value1, T2 value2, T3 value3, T4 value4) {
		this.value1 = value1;
		this.value2 = value2;
		this.value3 = value3;
		this.value4 = value4;
	}

	public T1 getValue1() {
		return value1;
	}

	public T2 getValue2() {
		return value2;
	}

	public T3 getValue3() {
		return value3;
	}

	public T4 getValue4() {
		return value4;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Tuple4<?, ?, ?, ?> tuple4 = (Tuple4<?, ?, ?, ?>) o;
		return Objects.equals(value1, tuple4.value1) &&
				Objects.equals(value2, tuple4.value2) &&
				Objects.equals(value3, tuple4.value3) &&
				Objects.equals(value4, tuple4.value4);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value1, value2, value3, value4);
	}
}
