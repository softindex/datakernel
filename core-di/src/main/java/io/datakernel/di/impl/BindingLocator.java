package io.datakernel.di.impl;

import io.datakernel.di.Key;
import io.datakernel.di.binding.Binding;
import io.datakernel.di.binding.BindingGenerator;
import org.jetbrains.annotations.Nullable;

/**
 * This function is passed to a {@link BindingGenerator generator} when trying to generate a binding.
 * <p>
 * Generators can depend on other bindings that could not be present but can be generated.
 * This function is used as a mean of recursion - when no requested binding is present it tries to generate it,
 * and it is called from the generator itself.
 */
@FunctionalInterface
public interface BindingLocator {
	/**
	 * Retrieves existing binding for given key or tries to recursively generate it from known {@link BindingGenerator generators}.
	 */
	@Nullable <T> Binding<T> get(Key<T> key);
}
