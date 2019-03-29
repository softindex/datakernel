package io.datakernel.util.ref;

public final class CharRef {
	private char peer;

	public CharRef(char peer) {
		this.peer = peer;
	}

	public char inc() {
		return ++peer;
	}

	public char inc(char add) {
		return peer += add;
	}

	public char dec() {
		return --peer;
	}

	public char dec(char sub) {
		return peer -= sub;
	}

	public char get() {
		return peer;
	}

	public void set(char peer) {
		this.peer = peer;
	}

	@Override
	public String toString() {
		return "→" + peer;
	}
}

