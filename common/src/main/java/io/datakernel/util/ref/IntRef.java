package io.datakernel.util.ref;

public final class IntRef {
	private int peer;

	public IntRef(int peer) {
		this.peer = peer;
	}

	public int inc() {
		return ++peer;
	}

	public int inc(int add) {
		return peer += add;
	}

	public int dec() {
		return --peer;
	}

	public int dec(int sub) {
		return peer -= sub;
	}

	public int get() {
		return peer;
	}

	public void set(int peer) {
		this.peer = peer;
	}

	@Override
	public String toString() {
		return "→" + peer;
	}
}

