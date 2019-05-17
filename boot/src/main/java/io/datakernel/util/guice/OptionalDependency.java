package io.datakernel.util.guice;

import io.datakernel.di.Inject;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;
import java.util.function.Supplier;

public final class OptionalDependency<T> {
	@Inject(optional = true)
	@Nullable
	private T value;

	@Nullable
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
