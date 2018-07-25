package io.datakernel.util;

import java.util.Objects;

public final class Tuple3<T1, T2, T3> {
	private final T1 value1;
	private final T2 value2;
	private final T3 value3;

	public Tuple3(T1 value1, T2 value2, T3 value3) {
		this.value1 = value1;
		this.value2 = value2;
		this.value3 = value3;
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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Tuple3<?, ?, ?> tuple3 = (Tuple3<?, ?, ?>) o;
		return Objects.equals(value1, tuple3.value1) &&
				Objects.equals(value2, tuple3.value2) &&
				Objects.equals(value3, tuple3.value3);
	}

	@Override
	public int hashCode() {

		return Objects.hash(value1, value2, value3);
	}
}
