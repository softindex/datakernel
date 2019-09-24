package io.global.ot.value;

import io.datakernel.ot.OTState;

public final class ChangeValueContainer<T> implements OTState<ChangeValue<T>> {
	private final T initial;
	private T value;

	private ChangeValueContainer(T value) {
		this.initial = value;
	}

	public static <T> ChangeValueContainer<T> empty() {
		return new ChangeValueContainer<>(null);
	}

	public static <T> ChangeValueContainer<T> of(T initial) {
		return new ChangeValueContainer<>(initial);
	}

	public T getValue() {
		return value;
	}

	@Override
	public void init() {
		value = initial;
	}

	@Override
	public void apply(ChangeValue<T> op) {
		value = op.getNext();
	}
}
