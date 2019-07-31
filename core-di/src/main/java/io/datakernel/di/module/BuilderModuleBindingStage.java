package io.datakernel.di.module;

import io.datakernel.di.core.*;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * This interface is used to restrict the DSL.
 * Basically, it disallows any methods from {@link BuilderModule} not listed below
 * to be called without previously calling {@link #bind bind(...)}.
 */
@SuppressWarnings("UnusedReturnValue")
public interface BuilderModuleBindingStage extends Module {
	<T> BuilderModule<T> bind(Key<T> key);

	<T> BuilderModule<T> bind(Class<T> cls);

	BuilderModuleBindingStage install(Module... modules);

	BuilderModuleBindingStage install(Collection<Module> modules);

	BuilderModuleBindingStage scan(Object container);

	BuilderModuleBindingStage scanStatics(Class<?> container);

	<S, E extends S> BuilderModuleBindingStage bindIntoSet(Key<S> setOf, Binding<E> binding);

	<S, E extends S> BuilderModuleBindingStage bindIntoSet(Key<S> setOf, Key<E> item);

	<S, E extends S> BuilderModuleBindingStage bindIntoSet(@NotNull Key<S> setOf, @NotNull E element);

	BuilderModuleBindingStage postInjectInto(Key<?> key);

	BuilderModuleBindingStage postInjectInto(Class<?> type);

	<E> BuilderModuleBindingStage transform(int priority, BindingTransformer<E> bindingTransformer);

	<E> BuilderModuleBindingStage generate(Class<?> pattern, BindingGenerator<E> bindingGenerator);

	<E> BuilderModuleBindingStage multibind(Key<E> key, Multibinder<E> multibinder);
}
