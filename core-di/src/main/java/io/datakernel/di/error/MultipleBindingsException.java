package io.datakernel.di.error;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import io.datakernel.di.LocationInfo;

import java.util.Set;

import static java.util.stream.Collectors.joining;

public final class MultipleBindingsException extends IllegalStateException {
	private final Key<?> key;
	private final Set<Binding<?>> bindings;

	public MultipleBindingsException(Key<?> key, Set<Binding<?>> bindings) {
		super(bindings.stream()
				.map(binding -> {
					LocationInfo location = binding.getLocation();
					if (location == null) {
						return "at <unknown binding location>";
					}
					return "\tat " + location.getDeclaration();
				})
				.collect(joining("\n", "for key " + key + ":\n", "\n")));
		this.key = key;
		this.bindings = bindings;
	}

	public Key<?> getKey() {
		return key;
	}

	public Set<Binding<?>> getBindings() {
		return bindings;
	}
}
