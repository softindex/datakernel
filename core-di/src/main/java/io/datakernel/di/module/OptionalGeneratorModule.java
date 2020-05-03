package io.datakernel.di.module;

import io.datakernel.di.binding.Binding;

import java.util.Optional;

/**
 * Extension module.
 * <p>
 * A binding of <code>Optional&lt;T&gt;</code> for any type <code>T</code> is generated,
 * with the resulting optional being empty if no binding for <code>T</code> was bound
 * or containing an instance of <code>T</code>
 */
public final class OptionalGeneratorModule extends AbstractModule {
	private OptionalGeneratorModule() {
	}

	public static OptionalGeneratorModule create() {
		return new OptionalGeneratorModule();
	}

	@Override
	protected void configure() {
		generate(Optional.class, (bindings, scope, key) -> {
			Binding<?> binding = bindings.get(key.getTypeParameter(0));
			return binding != null ?
					binding.mapInstance(Optional::of) :
					Binding.toInstance(Optional.empty());
		});
	}
}
