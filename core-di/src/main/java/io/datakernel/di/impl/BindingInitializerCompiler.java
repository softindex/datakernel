package io.datakernel.di.impl;

@FunctionalInterface
public interface BindingInitializerCompiler<R> {
	CompiledBindingInitializer<R> compile(CompiledBindingLocator compiledBindings);
}
