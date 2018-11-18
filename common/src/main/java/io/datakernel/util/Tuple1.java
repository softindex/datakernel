package io.datakernel.util;

import java.util.Objects;
import java.util.StringJoiner;

public final class Tuple1<T1> {
	private final T1 value1;

	public Tuple1(T1 value1) {
		this.value1 = value1;
	}

	public T1 getValue1() {
		return value1;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Tuple1<?> that = (Tuple1<?>) o;
		return Objects.equals(value1, that.value1);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value1);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", "{", "}")
				.add("" + value1)
				.toString();
	}
}
