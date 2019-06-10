package io.datakernel.di.core;

import io.datakernel.di.error.CannotGenerateBindingException;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

@FunctionalInterface
public interface BindingTransformer<T> {
	@NotNull Binding<T> transform(BindingProvider provider, Scope[] scope, Key<T> key, Binding<T> binding);

	@SuppressWarnings("unchecked")
	static BindingTransformer<?> combinedTransformer(Map<Integer, Set<BindingTransformer<?>>> transformers) {
		List<Set<BindingTransformer<?>>> transformerList = transformers.entrySet().stream()
				.sorted(Comparator.comparing(Map.Entry::getKey))
				.map(Map.Entry::getValue)
				.collect(toList());

		return (provider, scope, key, binding) -> {
			Binding<Object> result = binding;
			for (Set<BindingTransformer<?>> localTransformers : transformerList) {
				Set<@NotNull Binding<Object>> transformed = localTransformers.stream()
						.map(transformer -> ((BindingTransformer<Object>) transformer).transform(provider, scope, key, binding))
						.filter(b -> !b.equals(binding))
						.collect(toSet());

				if (transformed.isEmpty()) {
					continue;
				}
				if (transformed.size() > 1) {
					throw new CannotGenerateBindingException(key, "More than one transformer with the same priority transformed a binding");
				}
				result = transformed.iterator().next();
			}
			return result;
		};
	}
}
