package io.datakernel.di.module;

import io.datakernel.di.Binding;

import java.util.Set;
import java.util.function.Function;

@FunctionalInterface
public interface ConflictResolver<T> extends Function<Set<Binding<T>>, Binding<T>> {

	Binding<T> resolve(Set<Binding<T>> elements);

	@Override
	default Binding<T> apply(Set<Binding<T>> elements) {
		return resolve(elements);
	}
}
