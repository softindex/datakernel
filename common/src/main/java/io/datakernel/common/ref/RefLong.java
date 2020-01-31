package io.datakernel.common.ref;

public final class RefLong {
	public long value;

	public RefLong(long value) {
		this.value = value;
	}

	public long inc() {
		return ++value;
	}

	public long inc(long add) {
		return value += add;
	}

	public long dec() {
		return --value;
	}

	public long dec(long sub) {
		return value -= sub;
	}

	public long get() {
		return value;
	}

	public void set(long peer) {
		this.value = peer;
	}

	@Override
	public String toString() {
		return "→" + value;
	}
}

