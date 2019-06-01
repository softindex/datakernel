package io.datakernel.di.module;

import io.datakernel.di.Binding;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

@FunctionalInterface
public interface ConflictResolver<T> {

	Binding<T> resolve(Set<@NotNull Binding<T>> elements);
}
