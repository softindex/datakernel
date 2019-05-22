package io.datakernel.util.guice;

import io.datakernel.di.Inject;
import io.datakernel.util.Initializable;
import io.datakernel.util.Initializer;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

@Inject
public final class OptionalInitializer<T extends Initializable<T>> implements Initializer<T> {
	@Inject(optional = true)
	@Nullable
	private Initializer<T> initializer;

	@Inject(optional = true)
	@Nullable
	private Set<Initializer<T>> initializers;

	public boolean isPresent() {
		return initializer != null || (initializers != null && !initializers.isEmpty());
	}

	@Override
	public void accept(T value) {
		if (initializer != null) {
			initializer.accept(value);
		}
		if (initializers != null) {
			for (Initializer<T> initializer : initializers) {
				initializer.accept(value);
			}
		}
	}
}
