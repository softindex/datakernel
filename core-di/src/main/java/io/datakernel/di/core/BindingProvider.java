package io.datakernel.di.core;

import org.jetbrains.annotations.Nullable;

@FunctionalInterface
public interface BindingProvider {
	@Nullable <T> Binding<T> getBinding(Key<T> key);
}
