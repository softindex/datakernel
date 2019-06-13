package io.datakernel.di.core;

import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.util.stream.Collectors.toList;

@FunctionalInterface
public interface BindingTransformer<T> {
	BindingTransformer<?> IDENTITY = (provider, scope, key, binding) -> binding;

	@NotNull Binding<T> transform(BindingProvider provider, Scope[] scope, Key<T> key, Binding<T> binding);

	@SuppressWarnings("unchecked")
	static <T> BindingTransformer<T> identity() {
		return (BindingTransformer<T>) IDENTITY;
	}

	@SuppressWarnings("unchecked")
	static BindingTransformer<?> combinedTransformer(Map<Integer, Set<BindingTransformer<?>>> transformers) {

		List<Set<BindingTransformer<?>>> transformerList = transformers.entrySet().stream()
				.sorted(Comparator.comparing(Entry::getKey))
				.map(Entry::getValue)
				.collect(toList());

		return (provider, scope, key, binding) -> {
			Binding<Object> result = binding;

			for (Set<BindingTransformer<?>> localTransformers : transformerList) {

				Binding<Object> transformed = null;

				for (BindingTransformer<?> transformer : localTransformers) {
					Binding<Object> b = ((BindingTransformer<Object>) transformer).transform(provider, scope, key, result);
					if (b == binding) {
						continue;
					}
					if (transformed != null) {
						throw new DIException("More than one transformer with the same priority transformed a binding for key " + key.getDisplayString());
					}
					transformed = b;
				}
				if (transformed != null) {
					result = transformed;
				}
			}
			return result;
		};
	}
}
