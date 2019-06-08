package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

@FunctionalInterface
public interface Multibinder<T> {

	Binding<T> resolve(Set<@NotNull Binding<T>> elements);
}
