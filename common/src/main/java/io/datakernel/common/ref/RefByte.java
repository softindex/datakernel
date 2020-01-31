package io.datakernel.common.ref;

public final class RefByte {
	public byte value;

	public RefByte(byte value) {
		this.value = value;
	}

	public byte inc() {
		return ++value;
	}

	public byte inc(byte add) {
		return value += add;
	}

	public byte dec() {
		return --value;
	}

	public byte dec(byte sub) {
		return value -= sub;
	}

	public byte get() {
		return value;
	}

	public void set(byte peer) {
		this.value = peer;
	}

	@Override
	public String toString() {
		return "→" + value;
	}
}

