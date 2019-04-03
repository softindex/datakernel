package io.datakernel.util.ref;

public final class RefDouble {
	public double value;

	public RefDouble(double value) {
		this.value = value;
	}

	public double inc() {
		return ++value;
	}

	public double inc(double add) {
		return value += add;
	}

	public double dec() {
		return --value;
	}

	public double dec(double sub) {
		return value -= sub;
	}

	public double get() {
		return value;
	}

	public void set(double peer) {
		this.value = peer;
	}

	@Override
	public String toString() {
		return "→" + value;
	}
}

