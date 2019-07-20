package io.datakernel.di.core;

import org.jetbrains.annotations.NotNull;

public interface CompiledBindingLocator {
	@NotNull <Q> CompiledBinding<Q> locate(Key<Q> key);
}
