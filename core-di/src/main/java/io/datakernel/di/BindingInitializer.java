package io.datakernel.di;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Arrays.copyOfRange;
import static java.util.Collections.addAll;

public final class BindingInitializer<T> {
	private static final BindingInitializer<?> NOOP = new BindingInitializer<>(new Dependency[0], (instance, args) -> {});

	@FunctionalInterface
	public interface Initializer<T> {
		void apply(T instance, Object[] args);
	}

	private final Dependency[] dependencies;
	private final Initializer<T> initializer;

	private BindingInitializer(Dependency[] dependencies, Initializer<T> initializer) {
		this.dependencies = dependencies;
		this.initializer = initializer;
	}

	public Dependency[] getDependencies() {
		return dependencies;
	}

	public Initializer<T> getInitializer() {
		return initializer;
	}

	public static <T> BindingInitializer<T> of(Dependency[] dependencies, Initializer<T> initializer) {
		return new BindingInitializer<>(dependencies, initializer);
	}

	public static <T> BindingInitializer<T> of(Key<?>[] dependencies, Initializer<T> initializer) {
		return new BindingInitializer<>(Arrays.stream(dependencies).map(k -> new Dependency(k, true)).toArray(Dependency[]::new), initializer);
	}

	@SafeVarargs
	public static <T> BindingInitializer<T> combine(BindingInitializer<T>... bindingInitializers) {
		return combine(asList(bindingInitializers));
	}

	public static <T> BindingInitializer<T> combine(Collection<BindingInitializer<T>> bindingInitializers) {
		List<Initializer<T>> initializers = new ArrayList<>();
		List<Dependency> keys = new ArrayList<>();
		for (BindingInitializer<T> bi : bindingInitializers) {
			if (bi == NOOP) {
				continue;
			}
			Initializer<T> initializer = bi.getInitializer();
			int from = keys.size();
			int to = from + bi.getDependencies().length;
			addAll(keys, bi.getDependencies());
			initializers.add((instance, args) -> initializer.apply(instance, copyOfRange(args, from, to)));
		}
		if (initializers.isEmpty()) {
			return noop();
		}
		return BindingInitializer.of(keys.toArray(new Dependency[0]), (instance, args) -> initializers.forEach(initializer -> initializer.apply(instance, args)));
	}

	@SuppressWarnings("unchecked")
	public static <T> BindingInitializer<T> noop() {
		return (BindingInitializer<T>) NOOP;
	}
}
