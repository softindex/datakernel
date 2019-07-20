package io.datakernel.di.core;

@FunctionalInterface
public interface BindingCompiler<R> {
	CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, int level, int index);

	default CompiledBinding<R> compileForCreateOnly(CompiledBindingLocator compiledBindings) {
		return compile(compiledBindings, -1, -1);
	}
}
