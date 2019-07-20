package io.datakernel.di.impl;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Dependency;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;

/**
 * This is a {@link Binding} binding modifying function, that can add extra dependencies to it
 * and run initialization code for instance after it was created.
 */
public final class BindingInitializer<T> {
	private static final BindingInitializer<?> NOOP = new BindingInitializer<>(emptySet(), compiledBindings -> (instances, instance) -> {});

	private final Set<Dependency> dependencies;
	private final BindingInitializerCompiler<T> compiler;

	private BindingInitializer(Set<Dependency> dependencies, BindingInitializerCompiler<T> compiler) {
		this.dependencies = dependencies;
		this.compiler = compiler;
	}

	public Set<Dependency> getDependencies() {
		return dependencies;
	}

	public BindingInitializerCompiler<T> getCompiler() {
		return compiler;
	}

	public static <T> BindingInitializer<T> of(Set<Dependency> dependencies, BindingInitializerCompiler<T> bindingInitializerCompiler) {
		return new BindingInitializer<>(dependencies, bindingInitializerCompiler);
	}

	@SafeVarargs
	public static <T> BindingInitializer<T> combine(BindingInitializer<T>... bindingInitializers) {
		return combine(asList(bindingInitializers));
	}

	public static <T> BindingInitializer<T> combine(Collection<BindingInitializer<T>> bindingInitializers) {
		return new BindingInitializer<>(bindingInitializers.stream().map(BindingInitializer::getDependencies).flatMap(Collection::stream).collect(Collectors.toSet()),
				compiledBindings -> {
					//noinspection unchecked
					BiConsumer<AtomicReferenceArray[], T>[] initializers = bindingInitializers.stream()
							.filter(bindingInitializer -> bindingInitializer != NOOP)
							.map(bindingInitializer -> bindingInitializer.compiler.compile(compiledBindings))
							.toArray(BiConsumer[]::new);
					if (initializers.length == 0) return (instances, instance) -> {};
					if (initializers.length == 1) return initializers[0];
					if (initializers.length == 2) {
						BiConsumer<AtomicReferenceArray[], T> initializer0 = initializers[0];
						BiConsumer<AtomicReferenceArray[], T> initializer1 = initializers[1];
						return (instances, instance) -> {
							initializer0.accept(instances, instance);
							initializer1.accept(instances, instance);
						};
					}
					return (instances, instance) -> {
						for (BiConsumer<AtomicReferenceArray[], T> initializer : initializers) {
							initializer.accept(instances, instance);
						}
					};
				});
	}

	@SuppressWarnings("unchecked")
	public static <T> BindingInitializer<T> noop() {
		return (BindingInitializer<T>) NOOP;
	}
}
