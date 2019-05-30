package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;

import java.util.function.Function;

public interface BindingGenerator<T> {

	Binding<T> generate(Key<T> key, Function<Key<?>, Binding<?>> context);
}
