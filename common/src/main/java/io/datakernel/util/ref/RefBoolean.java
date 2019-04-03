package io.datakernel.util.ref;

public final class RefBoolean {
	public boolean value;

	public RefBoolean(boolean value) {
		this.value = value;
	}

	public boolean get() {
		return value;
	}

	public void set(boolean peer) {
		this.value = peer;
	}

	public boolean flip() {
		return value = !value;
	}

	public boolean or(boolean other) {
		return value |= other;
	}

	public boolean and(boolean other) {
		return value &= other;
	}

	@Override
	public String toString() {
		return "→" + value;
	}
}

