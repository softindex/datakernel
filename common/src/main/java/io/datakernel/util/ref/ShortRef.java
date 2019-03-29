package io.datakernel.util.ref;

public final class ShortRef {
	private short peer;

	public ShortRef(short peer) {
		this.peer = peer;
	}

	public short inc() {
		return ++peer;
	}

	public short inc(short add) {
		return peer += add;
	}

	public short dec() {
		return --peer;
	}

	public short dec(short sub) {
		return peer -= sub;
	}

	public short get() {
		return peer;
	}

	public void set(short peer) {
		this.peer = peer;
	}

	@Override
	public String toString() {
		return "→" + peer;
	}
}

