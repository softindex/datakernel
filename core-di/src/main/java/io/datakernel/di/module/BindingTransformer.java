package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Scope;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

@FunctionalInterface
public interface BindingTransformer<T> {
	@NotNull Binding<T> transform(BindingProvider provider, Scope[] scope, Key<T> key, Binding<T> binding);

	@SuppressWarnings("unchecked")
	static BindingTransformer<?> combinedTransformer(Map<Integer, BindingTransformer<?>> transformers) {
		List<BindingTransformer<?>> transformerList = transformers.entrySet().stream()
				.sorted(Comparator.comparing(Map.Entry::getKey))
				.map(Map.Entry::getValue)
				.collect(toList());

		return (provider, scope, key, binding) -> {
			for (BindingTransformer<?> transformer : transformerList) {
				binding = ((BindingTransformer<Object>) transformer).transform(provider, scope, key, binding);
			}
			return binding;
		};
	}
}
