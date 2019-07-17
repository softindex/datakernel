package io.datakernel.di.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import static java.util.Arrays.asList;

/**
 * This is a {@link Binding} binding modifying function, that can add extra dependencies to it
 * and run initialization code for instance after it was created.
 */
public final class BindingInitializer<T> {
	private static final BindingInitializer<?> NOOP = new BindingInitializer<>(new Dependency[0], (locator, instance) -> {});

	private final Dependency[] dependencies;
	private final BiConsumer<InstanceLocator, T> initializer;

	private BindingInitializer(Dependency[] dependencies, BiConsumer<InstanceLocator, T> initializer) {
		this.dependencies = dependencies;
		this.initializer = initializer;
	}

	public Dependency[] getDependencies() {
		return dependencies;
	}

	public BiConsumer<InstanceLocator, T> getInitializer() {
		return initializer;
	}

	public Binding<T> apply(Binding<T> binding) {
		if (this == NOOP) {
			return binding;
		}
		return binding
				.addDependencies(dependencies)
				.onInstance(initializer);
	}

	public static <T> BindingInitializer<T> of(BiConsumer<InstanceLocator, T> initializer, Dependency... dependencies) {
		return new BindingInitializer<>(dependencies, initializer);
	}

	@SafeVarargs
	public static <T> BindingInitializer<T> combine(BindingInitializer<T>... bindingInitializers) {
		return combine(asList(bindingInitializers));
	}

	public static <T> BindingInitializer<T> combine(Collection<BindingInitializer<T>> bindingInitializers) {
		List<BiConsumer<InstanceLocator, T>> initializers = new ArrayList<>();
		List<Dependency> keys = new ArrayList<>();
		for (BindingInitializer<T> bi : bindingInitializers) {
			if (bi == NOOP) {
				continue;
			}
			Collections.addAll(keys, bi.getDependencies());
			initializers.add(bi.getInitializer());
		}
		if (initializers.isEmpty()) {
			return noop();
		}
		return BindingInitializer.of(
				(locator, instance) -> initializers.forEach(initializer -> initializer.accept(locator, instance)),
				keys.toArray(new Dependency[0]));
	}

	@SuppressWarnings("unchecked")
	public static <T> BindingInitializer<T> noop() {
		return (BindingInitializer<T>) NOOP;
	}
}
