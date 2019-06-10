package io.datakernel.di.core;

import io.datakernel.di.error.CannotGenerateBindingException;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.datakernel.di.util.Types.findBestMatch;
import static java.util.stream.Collectors.toSet;

@FunctionalInterface
public interface BindingGenerator<T> {
	@Nullable Binding<T> generate(BindingProvider provider, Scope[] scope, Key<T> key);

	@SuppressWarnings("unchecked")
	static BindingGenerator<?> combinedGenerator(Map<Type, Set<BindingGenerator<?>>> generators) {
		return (provider, scope, key) -> {
			Set<BindingGenerator<?>> found = generators.get(findBestMatch(key.getType(), generators.keySet()));
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
