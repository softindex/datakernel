package io.datakernel.di.core;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;

@FunctionalInterface
public interface BindingInitializerCompiler<R> {
	BiConsumer<AtomicReferenceArray[], R> compile(CompiledBindingLocator compiledBindings);
}
