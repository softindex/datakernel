package io.datakernel.common.ref;

public final class Ref<T> {
	public T value;

	public Ref(T value) {
		this.value = value;
	}

	public Ref() {
		this(null);
	}

	public T get() {
		return value;
	}

	public void set(T peer) {
		this.value = peer;
	}

	public void unset() {
		this.value = null;
	}

	@Override
	public String toString() {
		return "→" + value;
	}
}
