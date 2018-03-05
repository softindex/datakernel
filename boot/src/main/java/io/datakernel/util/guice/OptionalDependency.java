package io.datakernel.util.guice;

import com.google.inject.Inject;

import java.util.function.Consumer;
import java.util.function.Supplier;

public final class OptionalDependency<T> {
	@Inject(optional = true)
	private T value;

	public T get() {
		return value;
	}

	public boolean isPresent() {
		return value != null;
	}

	public void ifPresent(Consumer<? super T> consumer) {
		if (value != null) {
			consumer.accept(value);
		}
	}

	public T orElse(T defaultValue) {
		return value != null ? value : defaultValue;
	}

	public T orElseGet(Supplier<T> defaultValue) {
		return value != null ? value : defaultValue.get();
	}
}
