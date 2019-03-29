package io.datakernel.util.ref;

public final class ByteRef {
	private byte peer;

	public ByteRef(byte peer) {
		this.peer = peer;
	}

	public byte inc() {
		return ++peer;
	}

	public byte inc(byte add) {
		return peer += add;
	}

	public byte dec() {
		return --peer;
	}

	public byte dec(byte sub) {
		return peer -= sub;
	}

	public byte get() {
		return peer;
	}

	public void set(byte peer) {
		this.peer = peer;
	}

	@Override
	public String toString() {
		return "→" + peer;
	}
}

