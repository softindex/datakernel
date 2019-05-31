package io.datakernel.di;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Arrays.copyOfRange;
import static java.util.Collections.addAll;

public final class BindingInitializer<T> {
	private static final BindingInitializer<?> IDENTITY = new BindingInitializer<>(new Dependency[0], (instance, args) -> {});

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

	@SuppressWarnings("unchecked")
	public static <T> BindingInitializer<T> identity() {
		return (BindingInitializer<T>) IDENTITY;
	}

	@SafeVarargs
	public static <T> BindingInitializer<T> combine(BindingInitializer<T>... bindingInitializers) {
		return combine(asList(bindingInitializers));
	}

	public static <T> BindingInitializer<T> combine(Collection<BindingInitializer<T>> bindingInitializers) {
		List<BindingInitializer<T>> bindingInitializersList = new ArrayList<>();
		List<Initializer<T>> initializers = new ArrayList<>();
		List<Dependency> keys = new ArrayList<>();
		for (BindingInitializer<T> bi : bindingInitializers) {
			if (bi == identity()) continue;
			bindingInitializersList.add(bi);
			Initializer<T> initializer = bi.getInitializer();
			int from = keys.size();
			int to = from + bi.getDependencies().length;
			addAll(keys, bi.getDependencies());
			initializers.add((instance, args) -> initializer.apply(instance, copyOfRange(args, from, to)));
		}
		if (bindingInitializersList.isEmpty()) return identity();
		if (bindingInitializersList.size() == 1) return bindingInitializersList.get(0);
		@SuppressWarnings("unchecked") Initializer<T>[] initializersArray = initializers.toArray(new Initializer[0]);
		return BindingInitializer.of(
				keys.toArray(new Dependency[0]),
				(instance, args) -> {
					for (Initializer<T> initializer : initializersArray) {
						initializer.apply(instance, args);
					}
				});
	}

}
