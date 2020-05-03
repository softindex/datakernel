package io.datakernel.di.impl;

import io.datakernel.di.Key;
import org.jetbrains.annotations.NotNull;

public interface CompiledBindingLocator {
	@NotNull <Q> CompiledBinding<Q> get(Key<Q> key);
}
