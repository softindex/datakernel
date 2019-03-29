package io.datakernel.util.ref;

public final class DoubleRef {
	private double peer;

	public DoubleRef(double peer) {
		this.peer = peer;
	}

	public double inc() {
		return ++peer;
	}

	public double inc(double add) {
		return peer += add;
	}

	public double dec() {
		return --peer;
	}

	public double dec(double sub) {
		return peer -= sub;
	}

	public double get() {
		return peer;
	}

	public void set(double peer) {
		this.peer = peer;
	}

	@Override
	public String toString() {
		return "→" + peer;
	}
}

