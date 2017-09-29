package io.datakernel.ot.utils;

public class TestAdd implements TestOp {
	private final int delta;

	public TestAdd(int delta) {
		this.delta = delta;
	}

	public TestAdd inverse() {
		return new TestAdd(-delta);
	}

	public int getDelta() {
		return delta;
	}

	@Override
	public String toString() {
		return String.valueOf(delta);
	}

	@Override
	public int apply(int value) {
		return value + delta;
	}
}
