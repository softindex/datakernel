package io.datakernel.di.error;

import io.datakernel.di.Injector;
import io.datakernel.di.Key;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public final class CyclicDependensiesException extends IllegalStateException {
	private final Injector injector;
	private final Set<Key<?>[]> cycles;

	public CyclicDependensiesException(Injector injector, Set<Key<?>[]> cycles) {
		super(cycles.stream()
				.map(cycle ->
						Stream.concat(Arrays.stream(cycle), Stream.of(cycle[0]))
								.map(Key::getDisplayString)
								.collect(joining(" -> ", "\t", " -> ...")))
				.collect(joining("\n", "\n", "\n")));
		this.injector = injector;
		this.cycles = cycles;
	}

	public Injector getInjector() {
		return injector;
	}

	public Set<Key<?>[]> getCycles() {
		return cycles;
	}
}
