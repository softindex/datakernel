package io.datakernel.common.ref;

public final class RefFloat {
	public float value;

	public RefFloat(float value) {
		this.value = value;
	}

	public float inc() {
		return ++value;
	}

	public float inc(float add) {
		return value += add;
	}

	public float dec() {
		return --value;
	}

	public float dec(float sub) {
		return value -= sub;
	}

	public float get() {
		return value;
	}

	public void set(float peer) {
		this.value = peer;
	}

	@Override
	public String toString() {
		return "→" + value;
	}
}

