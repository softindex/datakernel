package io.datakernel.di.impl;

import org.jetbrains.annotations.Nullable;

@FunctionalInterface
public interface BindingCompiler<R> {

	CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot);
}
