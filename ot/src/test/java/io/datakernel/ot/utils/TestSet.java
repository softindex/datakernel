package io.datakernel.ot.utils;

import static io.datakernel.util.Preconditions.checkState;

public class TestSet implements TestOp {
	private final int prev;
	private final int next;

	public TestSet(int prev, int next) {
		this.prev = prev;
		this.next = next;
	}

	public TestSet inverse() {
		return new TestSet(next, prev);
	}

	public int getPrev() {
		return prev;
	}

	public int getNext() {
		return next;
	}

	@Override
	public String toString() {
		return prev + ":=" + next;
	}

	@Override
	public int apply(int prev) {
		checkState(prev == this.prev);
		return next;
	}
}
