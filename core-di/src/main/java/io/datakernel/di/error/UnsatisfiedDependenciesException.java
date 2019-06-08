package io.datakernel.di.error;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.util.LocationInfo;

import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.joining;

public final class UnsatisfiedDependenciesException extends IllegalStateException {
	private final Injector injector;
	private final Map<Key<?>, Set<Binding<?>>> unsatisfiedDependencies;

	public UnsatisfiedDependenciesException(Injector injector, Map<Key<?>, Set<Binding<?>>> unsatisfiedDependencies) {
		super(unsatisfiedDependencies.entrySet().stream()
				.map(entry -> entry.getValue().stream()
						.map(binding -> {
							LocationInfo location = binding.getLocation();
							return "at " + (location != null ? location.getDeclaration() : "<unknown binding location>");
						})
						.collect(joining("\n\t\t     and ", "\tkey " + entry.getKey() + "\n\t\trequired ", "")))
				.collect(joining("\n", "\n", "\n")));
		this.injector = injector;
		this.unsatisfiedDependencies = unsatisfiedDependencies;
	}

	public Injector getInjector() {
		return injector;
	}

	public Map<Key<?>, Set<Binding<?>>> getUnsatisfiedDependensies() {
		return unsatisfiedDependencies;
	}
}
