package io.datakernel.ot.utils;

import io.datakernel.async.Promise;
import io.datakernel.ot.OTState;

public class TestOpState implements OTState<TestOp> {
	private int value;

	@Override
	public Promise<Void> init() {
		value = 0;
		return Promise.complete();
	}

	@Override
	public Promise<Void> apply(TestOp testOp) {
		value = testOp.apply(value);
		return Promise.complete();
	}

	public int getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "" + value;
	}
}
