package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Scope;
import org.jetbrains.annotations.Nullable;

@FunctionalInterface
public interface BindingGenerator<T> {
	@Nullable Binding<T> generate(Scope[] scope, Key<T> key, BindingProvider provider);
}
