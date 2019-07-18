package io.datakernel.di.core;

import java.util.*;
import java.util.function.BiConsumer;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;

/**
 * This is a {@link Binding} binding modifying function, that can add extra dependencies to it
 * and run initialization code for instance after it was created.
 */
public final class BindingInitializer<T> {
	private static final BindingInitializer<?> NOOP = new BindingInitializer<>(emptySet(), (locator, instance) -> {});

	private final Set<Dependency> dependencies;
	private final BiConsumer<InstanceLocator, T> initializer;

	private BindingInitializer(Set<Dependency> dependencies, BiConsumer<InstanceLocator, T> initializer) {
		this.dependencies = dependencies;
		this.initializer = initializer;
	}

	public Set<Dependency> getDependencies() {
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

	public static <T> BindingInitializer<T> of(Set<Dependency> dependencies, BiConsumer<InstanceLocator, T> initializer) {
		return new BindingInitializer<>(dependencies, initializer);
	}

	@SafeVarargs
	public static <T> BindingInitializer<T> combine(BindingInitializer<T>... bindingInitializers) {
		return combine(asList(bindingInitializers));
	}

	public static <T> BindingInitializer<T> combine(Collection<BindingInitializer<T>> bindingInitializers) {
		List<BiConsumer<InstanceLocator, T>> initializers = new ArrayList<>();
		Set<Dependency> dependencies = new HashSet<>();
		for (BindingInitializer<T> bi : bindingInitializers) {
			if (bi == NOOP) {
				continue;
			}
			dependencies.addAll(bi.getDependencies());
			initializers.add(bi.getInitializer());
		}
		if (initializers.isEmpty()) {
			return noop();
		}
		return BindingInitializer.of(dependencies, (locator, instance) -> initializers.forEach(initializer -> initializer.accept(locator, instance)));
	}

	@SuppressWarnings("unchecked")
	public static <T> BindingInitializer<T> noop() {
		return (BindingInitializer<T>) NOOP;
	}
}
