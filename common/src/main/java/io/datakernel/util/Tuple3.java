package io.datakernel.util;

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
}
