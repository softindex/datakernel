package io.datakernel.di.impl;

import io.datakernel.di.core.Key;
import org.jetbrains.annotations.NotNull;

public interface CompiledBindingLocator {
	@NotNull <Q> CompiledBinding<Q> locate(Key<Q> key);
}
