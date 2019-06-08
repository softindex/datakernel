package io.datakernel.di.error;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import org.jetbrains.annotations.Nullable;

public final class CannotConstructException extends IllegalArgumentException {
	private final Injector injector;
	private final Key<?> key;
	@Nullable
	private final Binding<?> binding;

	public CannotConstructException(Injector injector, Key<?> key, @Nullable Binding<?> binding) {
		super((binding != null ? "Binding refused to" : "No binding to") + " construct an instance for key " + key.getDisplayString());
		this.injector = injector;
		this.key = key;
		this.binding = binding;
	}

	public Injector getInjector() {
		return injector;
	}

	public Key<?> getKey() {
		return key;
	}

	@Nullable
	public Binding<?> getBinding() {
		return binding;
	}
}
