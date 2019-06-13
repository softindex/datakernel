package io.datakernel.di.core;

import io.datakernel.di.util.Types;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

@FunctionalInterface
public interface BindingGenerator<T> {
	BindingGenerator<Object> REFUSING = (provider, scope, key) -> null;

	@Nullable Binding<T> generate(BindingProvider provider, Scope[] scope, Key<T> key);

	@SuppressWarnings("unchecked")
	static <T> BindingGenerator<T> refusing() {
		return (BindingGenerator<T>) REFUSING;
	}

	@SuppressWarnings("unchecked")
	static BindingGenerator<?> combinedGenerator(Map<Class<?>, Set<BindingGenerator<?>>> generators) {
		return (provider, scope, key) -> {
			Class<Object> rawType = key.getRawType();
			Class<?> generatorKey = rawType.isInterface() ? rawType : Types.findClosestAncestor(rawType, generators.keySet());
			if (generatorKey == null) {
				return null;
			}
			Set<BindingGenerator<?>> found = generators.get(generatorKey);
			if (found == null) {
				return null;
			}

			Set<Binding<Object>> generatedBindings = found.stream()
					.map(generator -> ((BindingGenerator<Object>) generator).generate(provider, scope, key))
					.filter(Objects::nonNull)
					.collect(toSet());

			switch (generatedBindings.size()) {
				case 0:
					return null;
				case 1:
					return generatedBindings.iterator().next();
				default:
					throw new DIException("More than one generator provided a binding for key " + key.getDisplayString());
			}
		};
	}
}
