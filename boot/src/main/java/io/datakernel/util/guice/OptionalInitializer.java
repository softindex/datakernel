package io.datakernel.util.guice;

import com.google.inject.Inject;
import io.datakernel.util.Initializable;
import io.datakernel.util.Initializer;

import java.util.Set;

public final class OptionalInitializer<T extends Initializable<T>> implements Initializer<T> {
	@Inject(optional = true)
	private Initializer<T> initializer;

	@Inject(optional = true)
	private Set<Initializer<T>> initializers;

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
