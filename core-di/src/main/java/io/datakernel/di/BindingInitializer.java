package io.datakernel.di;

import java.util.*;

import static java.util.Arrays.copyOfRange;

public final class BindingInitializer<T> {
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

	public static <T> BindingInitializer<T> of(Dependency[] dependencies, Initializer<T> initializer) {
		return new BindingInitializer<>(dependencies, initializer);
	}

	public static <T> BindingInitializer<T> of(Key<?>[] dependencies, Initializer<T> initializer) {
		return new BindingInitializer<>(Arrays.stream(dependencies).map(k -> new Dependency(k, true)).toArray(Dependency[]::new), initializer);
	}

	public Dependency[] getDependencies() {
		return dependencies;
	}

	public Initializer<T> getInitializer() {
		return initializer;
	}

	public static <T> BindingInitializer<T> combine(Collection<BindingInitializer<T>> bindingInitializers) {
		if (bindingInitializers.size() == 1) {
			return bindingInitializers.iterator().next();
		}
		List<Initializer<T>> initializers = new ArrayList<>();
		List<Dependency> keys = new ArrayList<>();
		for (BindingInitializer<T> bindingInitializer : bindingInitializers) {
			int offset = keys.size();
			int count = bindingInitializer.getDependencies().length;
			Collections.addAll(keys, bindingInitializer.getDependencies());
			initializers.add((instance, args) ->
					bindingInitializer.getInitializer().apply(instance, copyOfRange(args, offset, count)));
		}
		return BindingInitializer.of(keys.toArray(new Dependency[0]), (instance, args) ->
				initializers.forEach(initializer -> initializer.apply(instance, args)));
	}
}
