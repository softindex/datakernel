package io.datakernel.util.ref;

public final class BooleanRef {
	private boolean peer;

	public BooleanRef(boolean peer) {
		this.peer = peer;
	}

	public boolean get() {
		return peer;
	}

	public void set(boolean peer) {
		this.peer = peer;
	}

	public boolean flip() {
		return peer = !peer;
	}

	public boolean or(boolean other) {
		return peer |= other;
	}

	public boolean and(boolean other) {
		return peer &= other;
	}

	@Override
	public String toString() {
		return "→" + peer;
	}
}

