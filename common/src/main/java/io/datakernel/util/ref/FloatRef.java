package io.datakernel.util.ref;

public final class FloatRef {
	private float peer;

	public FloatRef(float peer) {
		this.peer = peer;
	}

	public float inc() {
		return ++peer;
	}

	public float inc(float add) {
		return peer += add;
	}

	public float dec() {
		return --peer;
	}

	public float dec(float sub) {
		return peer -= sub;
	}

	public float get() {
		return peer;
	}

	public void set(float peer) {
		this.peer = peer;
	}

	@Override
	public String toString() {
		return "→" + peer;
	}
}

