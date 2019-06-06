package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import org.jetbrains.annotations.Nullable;

@FunctionalInterface
public interface BindingProvider {
	@Nullable <T> Binding<T> getBinding(Key<T> key);
}
