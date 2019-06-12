package io.datakernel.di.core;

import io.datakernel.di.error.CannotGenerateBindingException;
import io.datakernel.di.util.Types;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

@FunctionalInterface
public interface BindingGenerator<T> {
	@Nullable Binding<T> generate(BindingProvider provider, Scope[] scope, Key<T> key);

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

			if (generatedBindings.isEmpty()) {
				return null;
			}

			if (generatedBindings.size() > 1) {
				throw new CannotGenerateBindingException(key, "More than one generator provided a binding");
			}

			return generatedBindings.iterator().next();
		};
	}
}
