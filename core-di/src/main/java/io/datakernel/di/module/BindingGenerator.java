package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import io.datakernel.di.Scope;

@FunctionalInterface
public interface BindingGenerator<T> {

	Binding<T> generate(Scope[] scope, Key<T> key, BindingProvider provider);
}
