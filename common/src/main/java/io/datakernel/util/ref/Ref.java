package io.datakernel.util.ref;

public final class Ref<T> {
	private T peer;

	public Ref(T peer) {
		this.peer = peer;
	}

	public Ref() {
		this(null);
	}

	public T get() {
		return peer;
	}

	public void set(T peer) {
		this.peer = peer;
	}

	public void unset() {
		this.peer = null;
	}

	@Override
	public String toString() {
		return "→" + peer;
	}
}
