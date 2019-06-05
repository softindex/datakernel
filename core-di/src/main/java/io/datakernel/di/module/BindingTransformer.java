package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import io.datakernel.di.Scope;

@FunctionalInterface
public interface BindingTransformer<T> {
	Binding<T> transform(Scope[] scope, Key<T> key, Binding<T> binding, BindingProvider context);
}
