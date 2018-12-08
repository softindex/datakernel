package io.datakernel.ot.utils;

import io.datakernel.ot.OTState;

public class TestOpState implements OTState<TestOp> {
	private int value;

	@Override
	public void init() {
		value = 0;
	}

	@Override
	public void apply(TestOp testOp) {
		value = testOp.apply(value);
	}

	public int getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "" + value;
	}
}
