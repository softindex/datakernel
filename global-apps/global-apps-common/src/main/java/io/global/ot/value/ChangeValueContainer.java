package io.global.ot.value;

import io.datakernel.ot.OTState;

public final class ChangeValueContainer<T> implements OTState<ChangeValue<T>> {
	private T value;

	private ChangeValueContainer(T value) {
		this.value = value;
	}

	public static <T> ChangeValueContainer<T> empty() {
		return new ChangeValueContainer<>(null);
	}

	public T getValue() {
		return value;
	}

	@Override
	public void init() {
		value = null;
	}

	@Override
	public void apply(ChangeValue<T> op) {
		value = op.getNext();
	}
}
