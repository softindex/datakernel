package io.datakernel.util.ref;

public final class LongRef {
	private long peer;

	public LongRef(long peer) {
		this.peer = peer;
	}

	public long inc() {
		return ++peer;
	}

	public long inc(long add) {
		return peer += add;
	}

	public long dec() {
		return --peer;
	}

	public long dec(long sub) {
		return peer -= sub;
	}

	public long get() {
		return peer;
	}

	public void set(long peer) {
		this.peer = peer;
	}

	@Override
	public String toString() {
		return "→" + peer;
	}
}

