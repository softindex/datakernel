package io.datakernel.util;

public class Ref<T> {
	public T value;

	public Ref() {
	}

	public Ref(T value) {
		this.value = value;
	}

	public T get() {
		return value;
	}

	public void set(T value) {
		this.value = value;
	}
}
