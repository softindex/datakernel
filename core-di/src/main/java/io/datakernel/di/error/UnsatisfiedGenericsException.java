package io.datakernel.di.error;

import java.lang.reflect.TypeVariable;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

public final class UnsatisfiedGenericsException extends IllegalStateException {
	private final Set<TypeVariable<?>> unsatisfiedGenerics;

	public UnsatisfiedGenericsException(Set<TypeVariable<?>> unsatisfiedGenerics) {
		super("Actual types for generics " + unsatisfiedGenerics + " were not found in class hierarchy");
		this.unsatisfiedGenerics = unsatisfiedGenerics;
	}

	public Set<TypeVariable<?>> getUnsatisfiedGenerics() {
		return unsatisfiedGenerics;
	}
}
