package io.datakernel.util.ref;

public final class RefInt {
	public int value;

	public RefInt(int value) {
		this.value = value;
	}

	public int inc() {
		return ++value;
	}

	public int inc(int add) {
		return value += add;
	}

	public int dec() {
		return --value;
	}

	public int dec(int sub) {
		return value -= sub;
	}

	public int get() {
		return value;
	}

	public void set(int peer) {
		this.value = peer;
	}

	@Override
	public String toString() {
		return "→" + value;
	}
}

