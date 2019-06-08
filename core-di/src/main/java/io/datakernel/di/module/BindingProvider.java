package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import org.jetbrains.annotations.Nullable;

@FunctionalInterface
public interface BindingProvider {
	@Nullable <T> Binding<T> getBinding(Key<T> key);
}
