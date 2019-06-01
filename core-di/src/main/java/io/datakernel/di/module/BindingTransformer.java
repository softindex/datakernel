package io.datakernel.di.module;

import io.datakernel.di.BindingInitializer;
import io.datakernel.di.Key;

@FunctionalInterface
public interface BindingTransformer<T> {

	BindingInitializer<T> run(Key<T> key, BindingGenerationContext context);
}
