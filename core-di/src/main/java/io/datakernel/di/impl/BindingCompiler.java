package io.datakernel.di.impl;

@FunctionalInterface
public interface BindingCompiler<R> {
	CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, int level, int index);

	default CompiledBinding<R> compileForCreateOnly(CompiledBindingLocator compiledBindings, int level, int index) {
		return compile(compiledBindings, level, -1);
	}
}
