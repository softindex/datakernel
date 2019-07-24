package io.datakernel.di.impl;

@FunctionalInterface
public interface BindingCompiler<R> {
	CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, int scope, int index);

	default CompiledBinding<R> compileForCreateOnly(CompiledBindingLocator compiledBindings, int scope, int index) {
		return compile(compiledBindings, scope, -1);
	}
}
